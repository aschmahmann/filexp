package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/xerrors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	lotusbs "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/namesys"
	routingv1client "github.com/ipfs/boxo/routing/http/client"
	httpcontentrouter "github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p"
	madns "github.com/multiformats/go-multiaddr-dns"

	"github.com/urfave/cli/v2"

	ethabi "github.com/ethereum/go-ethereum/accounts/abi"
	ens "github.com/wealdtech/go-ens"
	ensresolver "github.com/wealdtech/go-ens/contracts/resolver"
)

var stateFlags = []cli.Flag{
	&cli.PathFlag{
		Name:  "car",
		Usage: "path to state CAR",
	},
	&cli.StringSliceFlag{
		Name:  "tipset-cids",
		Usage: "tipset CIDs for use when downloading state over the network",
	},
	&cli.BoolFlag{
		Name:  "trust-chainlove",
		Usage: "ask chain.love for the state from roughly 2 hours ago",
	},
}

func main() {
	app := &cli.App{
		Name:  "filexp",
		Usage: "explore filecoin state",
		Commands: []*cli.Command{
			{
				Name:        "msig-coins",
				Usage:       "<signer-key>",
				Description: "Add up all of the coins controlled by multisigs with the given signer and signing threshold of 1. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Flags:       stateFlags,
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					signerKey := args.Get(0)
					signerAddr, err := address.NewFromString(signerKey)
					if err != nil {
						return err
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}

					return getCoins(ctx.Context, bs, tsk, signerAddr)
				},
			},
			{
				Name:        "enumerate-actors",
				Description: "List all actors. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Flags: append([]cli.Flag{
					&cli.BoolFlag{
						Name:        "count-only",
						Value:       false,
						DefaultText: "will not emit the actor IDs, and just count them",
					},
				}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}
					return getActors(ctx.Context, bs, tsk, ctx.Bool("count-only"))
				},
			},
			{
				Name:        "get-balance",
				Description: "Get the balance for a given actor. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					actorAddrString := args.Get(0)
					actorAddr, err := address.NewFromString(actorAddrString)
					if err != nil {
						return err
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}

					return getBalance(ctx.Context, bs, tsk, actorAddr)
				},
			},
			{
				Name:        "fevm-exec",
				Description: "Execute a read-only FVM actor. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Usage:       "<eth-addr> <eth-data>",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					eaddr, err := ethtypes.ParseEthAddress(args.Get(0))
					if err != nil {
						return xerrors.Errorf("unable to parse eth address: %w", err)
					}
					decodedBytes, err := ethtypes.DecodeHexString(args.Get(1))
					if err != nil {
						return xerrors.Errorf("could not decode hex bytes: %w", err)
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}

					return fevmExec(ctx.Context, bs, tsk, &eaddr, decodedBytes)
				},
			},
			{
				Name:        "fevm-daemon",
				Description: "Start a daemon that will respond to Ethereum JSON RPC calls. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Usage:       "[port]",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					listenStr := "127.0.0.1:"
					if args.Len() != 0 {
						listenStr += args.First()
					}

					l, err := net.Listen("tcp", listenStr)
					if err != nil {
						return err
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}
					ebs := NewEphemeralBlockstore(bs)

					addr := "api.chain.love"
					var api lotusapi.FullNodeStruct
					closer, err := jsonrpc.NewMergeClient(ctx.Context, "ws://"+addr+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
					if err != nil {
						return fmt.Errorf("connecting with lotus API failed: %w", err)
					}
					defer closer()

					erpc := &ethRpcResolver{
						lbs:          ebs,
						tsk:          tsk,
						lastTskCheck: time.Now(),
						api:          api,
					}

					http.HandleFunc("/rpc/v1", func(writer http.ResponseWriter, request *http.Request) {
						if request.Method != http.MethodPost {
							writer.WriteHeader(http.StatusMethodNotAllowed)
							return
						}
						reqBody, err := io.ReadAll(request.Body)
						if err != nil {
							writer.WriteHeader(http.StatusInternalServerError)
							_, _ = writer.Write([]byte(err.Error()))
							return
						}

						type jsonRPC struct {
							Jsonrpc string            `json:"jsonrpc"`
							ID      int               `json:"id,omitempty"`
							Method  string            `json:"method"`
							Params  ethtypes.EthCall  `json:"params"`
							Meta    map[string]string `json:"meta,omitempty"`
						}

						reqMsg, err := jsonrpc.DecodeParams[jsonRPC](reqBody)
						if err != nil {
							writer.WriteHeader(http.StatusInternalServerError)
							_, _ = writer.Write([]byte(err.Error()))
							return
						}
						if reqMsg.Method != "eth_call" {
							writer.WriteHeader(http.StatusBadRequest)
							_, _ = writer.Write([]byte("only eth_call supported"))
							return
						}

						if reqMsg.Params.To == nil {
							writer.WriteHeader(http.StatusBadRequest)
							_, _ = writer.Write([]byte("must send to a valid address"))
							return
						}

						if reqMsg.Params.From != nil {
							writer.WriteHeader(http.StatusBadRequest)
							_, _ = writer.Write([]byte("can only send from null address"))
							return
						}

						eaddr := *reqMsg.Params.To
						methodData := reqMsg.Params.Data

						ret, err := erpc.Call(ctx.Context, eaddr, methodData)
						if err != nil {
							writer.WriteHeader(http.StatusInternalServerError)
							_, _ = writer.Write([]byte(fmt.Errorf("could not perform call operation: %w", err).Error()))
							return
						}

						type returnType struct {
							Jsonrpc string `json:"jsonrpc"`
							ID      int    `json:"id,omitempty"`
							Result  string `json:"result"`
						}

						sendResponse := returnType{
							Jsonrpc: "2.0",
							ID:      reqMsg.ID,
							Result:  ethtypes.EthBytes(ret).String(),
						}

						responseBytes, err := json.Marshal(sendResponse)
						if err != nil {
							writer.WriteHeader(http.StatusInternalServerError)
							_, _ = writer.Write([]byte(xerrors.Errorf("failed to marshal return value: %w", err).Error()))
							return
						}
						_, _ = writer.Write(responseBytes)
					})
					if err := http.Serve(l, nil); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:        "gateway-with-fns",
				Description: "Start a daemon that will respond to Ethereum JSON RPC calls. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Usage:       "[port]",
				Flags: append([]cli.Flag{
					&cli.StringFlag{
						Name:     "routing-v1-endpoint",
						Required: false,
						Value:    "http://127.0.0.1:8080",
					},
				}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					listenStr := "127.0.0.1:"
					if args.Len() != 0 {
						listenStr += args.First()
					}

					l, err := net.Listen("tcp", listenStr)
					if err != nil {
						return err
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}
					ebs := NewEphemeralBlockstore(bs)

					addr := "api.chain.love"
					var api lotusapi.FullNodeStruct
					closer, err := jsonrpc.NewMergeClient(ctx.Context, "ws://"+addr+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
					if err != nil {
						return fmt.Errorf("connecting with lotus API failed: %w", err)
					}
					defer closer()

					erpc := &ethRpcResolver{
						lbs:          ebs,
						tsk:          tsk,
						lastTskCheck: time.Now(),
						api:          api,
					}
					fnsDNS := &fnsResolver{e: erpc}

					nonChainBs := bstore.NewIdStore(bstore.NewBlockstore(datastore.NewMapDatastore()))
					h, err := libp2p.New()
					if err != nil {
						return err
					}

					r1, err := routingv1client.New(ctx.String("routing-v1-endpoint"))
					if err != nil {
						return err
					}

					vs := httpcontentrouter.NewContentRoutingClient(r1)

					nonChainBitswap := bsclient.New(ctx.Context, bsnet.NewFromIpfsHost(h, vs), nonChainBs)
					gatewayBsrv := blockservice.New(nonChainBs, nonChainBitswap)

					ns, err := namesys.NewNameSystem(vs, namesys.WithDNSResolver(fnsDNS))
					if err != nil {
						return err
					}
					backend, err := gateway.NewBlocksBackend(gatewayBsrv, gateway.WithValueStore(vs), gateway.WithNameSystem(ns))
					gwHandler := gateway.NewHandler(gateway.Config{
						DeserializedResponses: true,
					}, backend)

					fmt.Printf("serving IPFS Gateway API on %s", l.Addr())
					return http.Serve(l, gwHandler)
				},
			},
			{
				Name:        "fns-resolver",
				Description: "starts an fns.space based",
				Usage:       "<name>",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					eaddr, err := ethtypes.ParseEthAddress("0xed9bd04b1BB87Abe2EfF583A977514940c95699c")
					if err != nil {
						return xerrors.Errorf("unable to parse eth address: %w", err)
					}

					nameHash := ens.NameHash(args.First())
					parsed, err := ethabi.JSON(strings.NewReader(ensresolver.ResolverContractABI))
					if err != nil {
						return err
					}
					methodData, err := parsed.Pack("contenthash", nameHash)
					if err != nil {
						return err
					}

					bs, tsk, err := getState(ctx)
					if err != nil {
						return err
					}

					ts, sm, filMsg, err := getFevmRequest(ctx.Context, NewEphemeralBlockstore(bs), tsk, &eaddr, methodData)
					if err != nil {
						return err
					}

					for i := 1; i < 2; i++ {
						time.Sleep(time.Second * 10)
						fmt.Printf("it's been %d seconds\n", i*10)
					}

					res, err := sm.Call(ctx.Context, filMsg, ts)
					if err != nil {
						return xerrors.Errorf("unable to make a call: %w", err)
					}

					var ret abi.CborBytes
					if err := ret.UnmarshalCBOR(bytes.NewReader(res.MsgRct.Return)); err != nil {
						return xerrors.Errorf("failed to unmarshal return value: %w", err)
					}

					var contentHashData []byte
					err = parsed.Unpack(&contentHashData, "contenthash", ret)
					if err != nil {
						return xerrors.Errorf("unable to unpack result %x, %w", ret, err)
					}

					fmt.Println(string(contentHashData))

					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getState(ctx *cli.Context) (bstore.Blockstore, types.TipSetKey, error) {
	carSet := ctx.IsSet("car")
	tsSet := ctx.IsSet("tipset-cids")
	clSet := ctx.IsSet("trust-chainlove")

	if carSet {
		carLocation := ctx.String("car")
		if tsSet || clSet {
			return nil, types.EmptyTSK, fmt.Errorf("choose only one of CAR, tipset-cids, or trust-chainlove")
		}
		bs, tsk, err := getStateFromCar(ctx.Context, carLocation)
		if err != nil {
			return nil, types.EmptyTSK, err
		}
		return bs, tsk, nil
	}

	var tsk types.TipSetKey

	if tsSet {
		if clSet {
			return nil, types.EmptyTSK, fmt.Errorf("choose only one of CAR, tipset-cids, or trust-chainlove")
		}
		cidStrs := ctx.StringSlice("tipset-cids")
		var cids []cid.Cid
		for _, s := range cidStrs {
			c, err := cid.Decode(s)
			if err != nil {
				return nil, types.EmptyTSK, err
			}
			cids = append(cids, c)
		}
		tsk = types.NewTipSetKey(cids...)
	}

	if clSet {
		var height int64
		var err error
		height, tsk, err = getStableChainloveTSK(ctx.Context)
		if err != nil {
			log.Fatalf(err.Error())
		}
		fmt.Printf("using chainheight %d, with reported tipset %s\n", height, tsk)
	}

	bs, err := getStateDynamicallyLoadedFromBitswap(ctx.Context)
	if err != nil {
		return nil, types.EmptyTSK, err
	}

	return bs, tsk, err
}

func getStableChainloveTSK(ctx context.Context) (int64, types.TipSetKey, error) {
	addr := "api.chain.love"
	var api lotusapi.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, "ws://"+addr+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
	if err != nil {
		return 0, types.EmptyTSK, fmt.Errorf("connecting with lotus API failed: %w", err)
	}
	defer closer()

	chainHeightTwoHoursAgo := (time.Now().Add(-2*time.Hour).Unix() - 1598306400) / 30
	tipset, err := api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(chainHeightTwoHoursAgo), types.TipSetKey{})
	if err != nil {
		return 0, types.EmptyTSK, fmt.Errorf("could not get chain tipset from height %d: %w", chainHeightTwoHoursAgo, err)
	}
	return chainHeightTwoHoursAgo, tipset.Key(), nil
}

type ethRpcResolver struct {
	lbs          lotusbs.Blockstore
	tsk          types.TipSetKey
	tskMx        sync.RWMutex
	lastTskCheck time.Time
	api          lotusapi.FullNodeStruct
}

func (e *ethRpcResolver) Call(ctx context.Context, eaddr ethtypes.EthAddress, methodData ethtypes.EthBytes) ([]byte, error) {
	e.tskMx.RLock()
	updateTsk := false
	if time.Since(e.lastTskCheck) > time.Second*30 {
		updateTsk = true
	}
	e.tskMx.RUnlock()
	if updateTsk {
		e.tskMx.Lock()
		if time.Since(e.lastTskCheck) > time.Second*30 {
			ts, err := e.api.ChainHead(ctx)
			if err != nil {
				log.Printf("could not update tipset %s", err.Error())
			} else {
				e.lastTskCheck = time.Now()
				e.tsk = ts.Key()
			}
		}
		e.tskMx.Unlock()
	}

	ts, sm, filMsg, err := getFevmRequest(ctx, e.lbs, e.tsk, &eaddr, methodData)
	if err != nil {
		return nil, err
	}

	res, err := sm.Call(ctx, filMsg, ts)
	if err != nil {
		return nil, xerrors.Errorf("unable to make a call: %w", err)
	}

	var ret abi.CborBytes
	if err := ret.UnmarshalCBOR(bytes.NewReader(res.MsgRct.Return)); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal return value: %w", err)
	}

	return ret, nil
}

type fnsResolver struct {
	e *ethRpcResolver
}

func (f *fnsResolver) LookupIPAddr(ctx context.Context, s string) ([]net.IPAddr, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fnsResolver) LookupTXT(ctx context.Context, s string) ([]string, error) {
	resolvedName, err := f.ResolveFNS(ctx, s)
	if err != nil {
		return nil, err
	}

	return []string{
		fmt.Sprintf("dnslink=%s", resolvedName),
	}, nil
}

func (f *fnsResolver) ResolveFNS(ctx context.Context, s string) (string, error) {
	if !strings.HasSuffix(s, ".fil") {
		return "", fmt.Errorf("only .fil supported")
	}

	eaddr, err := ethtypes.ParseEthAddress("0xed9bd04b1BB87Abe2EfF583A977514940c95699c")
	if err != nil {
		return "", fmt.Errorf("unable to parse eth address: %w", err)
	}

	nameHash := ens.NameHash(s)
	parsed, err := ethabi.JSON(strings.NewReader(ensresolver.ResolverContractABI))
	if err != nil {
		return "", err
	}

	methodData, err := parsed.Pack("contenthash", nameHash)
	if err != nil {
		return "", err
	}

	ret, err := f.e.Call(ctx, eaddr, methodData)
	if err != nil {
		return "", err
	}

	var contentHashData []byte
	err = parsed.Unpack(&contentHashData, "contenthash", ret)
	if err != nil {
		return "", fmt.Errorf("unable to unpack result %x, %w", ret, err)
	}

	// TODO: use proper parsing with namespaces and binary keys
	stringPath := fmt.Sprintf("/ipfs/%s", string(contentHashData))
	return stringPath, nil
}

var _ madns.BasicResolver = (*fnsResolver)(nil)
