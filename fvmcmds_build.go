//go:build fvm

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	filaddr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	chainstore "github.com/filecoin-project/lotus/chain/store"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/cmd/lotus-sim/simulation/mock"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
)

func cmdFevmExec(cctx *cli.Context) error {
	args := cctx.Args()
	eaddr, err := ethtypes.ParseEthAddress(args.Get(0))
	if err != nil {
		return xerrors.Errorf("unable to parse eth address: %w", err)
	}
	decodedBytes, err := ethtypes.DecodeHexString(args.Get(1))
	if err != nil {
		return xerrors.Errorf("could not decode hex bytes: %w", err)
	}

	bg, ts, err := getAnchorPoint(cctx)
	if err != nil {
		return err
	}
	defer bg.LogStats()

	return fevmExec(cctx.Context, bg, ts, &eaddr, decodedBytes, cctx.Path("output"))
}

func cmdFevmDaemon(cctx *cli.Context) error {
	args := cctx.Args()
	ctx := cctx.Context
	listenStr := "127.0.0.1:"
	if args.Len() != 0 {
		listenStr += args.First()
	}

	l, err := net.Listen("tcp", listenStr)
	if err != nil {
		return err
	}

	bg, ts, err := getAnchorPoint(cctx)
	if err != nil {
		return err
	}
	defer bg.LogStats()

	var api fil.LotusDaemonAPIClientV0

	var rpcURL string
	if cctx.Bool("trust-chainlove") {
		rpcURL = chainLoveURL
	}
	if customRPC := cctx.String("rpc-endpoint"); customRPC != "" {
		rpcURL = customRPC
	}

	if rpcURL != "" {
		var apiCloser jsonrpc.ClientCloser
		api, apiCloser, err = fil.NewLotusDaemonAPIClientV0(ctx, rpcURL, 0, "")
		if err != nil {
			return err
		}
		defer apiCloser()
	}

	sm, err := newFilStateReader(bg)
	if err != nil {
		return err
	}

	erpc := &ethRpcResolver{
		sm:           sm,
		ts:           ts,
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

		ret, err := erpc.Call(ctx, eaddr, methodData)
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
}

type ethRpcResolver struct {
	sm           *lchstmgr.StateManager
	ts           *lchtypes.TipSet
	tsMx         sync.RWMutex
	lastTskCheck time.Time
	api          fil.LotusDaemonAPIClientV0
}

func (e *ethRpcResolver) Call(ctx context.Context, eaddr ethtypes.EthAddress, methodData ethtypes.EthBytes) ([]byte, error) {
	e.tsMx.RLock()
	updateTsk := false
	if time.Since(e.lastTskCheck) > time.Second*30 {
		updateTsk = true
	}
	callTS := e.ts
	e.tsMx.RUnlock()

	if updateTsk && e.api != nil {
		e.tsMx.Lock()
		if time.Since(e.lastTskCheck) > time.Second*30 {
			ts, err := e.api.ChainHead(ctx)
			if err != nil {
				log.Errorf("could not update tipset %s", err.Error())
			} else {
				e.lastTskCheck = time.Now()
				e.ts = ts
			}
		}
		callTS = e.ts
		e.tsMx.Unlock()
	}

	filMsg, err := getFevmRequest(&eaddr, methodData)
	if err != nil {
		return nil, err
	}

	res, err := e.sm.Call(ctx, filMsg, callTS)
	if err != nil {
		return nil, xerrors.Errorf("unable to make a call: %w", err)
	}

	var ret abi.CborBytes
	if err := ret.UnmarshalCBOR(bytes.NewReader(res.MsgRct.Return)); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal return value: %w", err)
	}

	return ret, nil
}

func fevmExec(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes, outputCAR string) error {
	filMsg, err := getFevmRequest(eaddr, edata)
	if err != nil {
		return err
	}
	log.Infof("epoch %s", ts.Height())

	sm, err := newFilStateReader(bg)
	if err != nil {
		return err
	}

	act, err := sm.LoadActor(ctx, filMsg.To, ts)
	if err != nil {
		return xerrors.Errorf("could not load actor the message is from: %w", err)
	}
	actorStateRoot := act.Head
	log.Infof("actor state root: %s", actorStateRoot)

	_, err = bg.Get(ctx, actorStateRoot)
	if err != nil {
		return fmt.Errorf("error loading state root %w", err)
	}

	for _, c := range bg.orderedCids {
		log.Infof("pre-call cid: %s", c)
	}

	res, err := sm.Call(ctx, filMsg, ts)
	if err != nil {
		return xerrors.Errorf("unable to make a call: %w", err)
	}

	str, err := json.Marshal(res)
	if err != nil {
		fmt.Println(res)
		return xerrors.Errorf("could not marshal response as json: %w", err)
	}

	fmt.Println(string(str))

	cidsInOrder := bg.orderedCids
	if err != nil {
		return err
	}

	for _, c := range cidsInOrder {
		fmt.Printf("all cid: %s\n", c)
	}

	if outputCAR != "" {
		carw, err := carbs.OpenReadWrite(outputCAR, []cid.Cid{actorStateRoot}, carbs.WriteAsCarV1(true))
		if err != nil {
			return err
		}

		for _, c := range cidsInOrder {
			blk, err := bg.Get(ctx, c)
			if err != nil {
				return err
			}
			if err := carw.Put(ctx, blk); err != nil {
				return err
			}
		}

		if err := carw.Finalize(); err != nil {
			return err
		}
	}

	return nil
}

func getFevmRequest(eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes) (*lchtypes.Message, error) {
	tx := ethtypes.EthCall{
		From:     nil,
		To:       eaddr,
		Gas:      0,
		GasPrice: ethtypes.EthBigInt{},
		Value:    ethtypes.EthBigInt{},
		Data:     edata,
	}
	filMsg, err := ethCallToFilecoinMessage(tx)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert ethcall to filecoin message: %w", err)
	}

	return filMsg, nil
}

func ethCallToFilecoinMessage(tx ethtypes.EthCall) (*lchtypes.Message, error) {
	var from filaddr.Address
	if tx.From == nil || *tx.From == (ethtypes.EthAddress{}) {
		// Send from the filecoin "system" address.
		var err error
		from, err = (ethtypes.EthAddress{}).ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to construct the ethereum system address: %w", err)
		}
	} else {
		// The from address must be translatable to an f4 address.
		var err error
		from, err = tx.From.ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to translate sender address (%s): %w", tx.From.String(), err)
		}
		if p := from.Protocol(); p != filaddr.Delegated {
			return nil, fmt.Errorf("expected a class 4 address, got: %d: %w", p, err)
		}
	}

	var params []byte
	if len(tx.Data) > 0 {
		initcode := abi.CborBytes(tx.Data)
		params2, err := actors.SerializeParams(&initcode)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize params: %w", err)
		}
		params = params2
	}

	var to filaddr.Address
	var method abi.MethodNum
	if tx.To == nil {
		// this is a contract creation
		to = filbuiltin.EthereumAddressManagerActorAddr
		method = filbuiltin.MethodsEAM.CreateExternal
	} else {
		addr, err := tx.To.ToFilecoinAddress()
		if err != nil {
			return nil, xerrors.Errorf("cannot get Filecoin address: %w", err)
		}
		to = addr
		method = filbuiltin.MethodsEVM.InvokeContract
	}

	return &lchtypes.Message{
		From:       from,
		To:         to,
		Value:      filbig.Int(tx.Value),
		Method:     method,
		Params:     params,
		GasLimit:   build.BlockGasLimit,
		GasFeeCap:  filbig.Zero(),
		GasPremium: filbig.Zero(),
	}, nil
}

func newFilStateReader(bsrc ipldcbor.IpldBlockstore) (*lchstmgr.StateManager, error) {
	ebs := NewEphemeralBlockstore(bsrc)
	mds := dssync.MutexWrap(ds.NewMapDatastore())
	c := cid.MustParse("bafy2bzacecnamqgqmifpluoeldx7zzglxcljo6oja4vrmtj7432rphldpdmm2")
	err := mds.Put(context.TODO(), ds.NewKey("0"), c.Bytes())
	if err != nil {
		return nil, err
	}

	cs := chainstore.NewChainStore(
		ebs,
		ebs,
		mds,
		nil,
		nil,
	)

	return lchstmgr.NewStateManager(
		cs,
		consensus.NewTipSetExecutor(filcns.RewardFunc),
		vm.Syscalls(mock.Verifier),
		filcns.DefaultUpgradeSchedule(),
		nil,
		mds,
		nil,
	)
}
