package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger(fmt.Sprintf("%s(%d)", "filexp", os.Getpid()))

// 20 is considered good for WdPoST: https://github.com/filecoin-project/builtin-actors/blob/v13.0.0/runtime/src/runtime/policy.rs#L290-L293
// nevertheless bump to 30 as per https://filecoinproject.slack.com/archives/C02D73MHM63/p1718762303033709?thread_ts=1718693790.469889&cid=C02D73MHM63
var defaultRpcLookbackEpochs = uint(30)

var stateFlags = []cli.Flag{
	&cli.PathFlag{
		Name:  "car",
		Usage: "Path to state snapshot CAR, tipset inferred from car root",
	},
	&cli.StringSliceFlag{
		Name:  "tipset-cids",
		Usage: "Specific tipset CIDs to get directly over libp2p",
	},
	&cli.StringFlag{
		Name:  "rpc-endpoint",
		Usage: "Filecoin RPC API endpoint to determine current tipset",
	},
	&cli.StringFlag{
		Name:  "rpc-fullnode",
		Usage: "Filecoin Full Node RPC API, to use as source of current tipset and all IPLD blocks",
	},
	&cli.UintFlag{
		Name:  "lookback-epochs",
		Usage: "How many epochs to look back when pulling state from the network",
		Value: defaultRpcLookbackEpochs,
		DefaultText: fmt.Sprintf("%d epochs / %s minutes",
			defaultRpcLookbackEpochs,
			strconv.FormatFloat(float64(defaultRpcLookbackEpochs*filbuiltin.EpochDurationSeconds)/60, 'f', -1, 64),
		),
	},
	&cli.BoolFlag{
		Name:  "trust-chainlove",
		Usage: "Equivalent to --rpc-endpoint=https://api.chain.love",
	},
}

func main() {
	logging.SetLogLevel("*", "INFO")
	// the network stack is incredibly chatty: silence it all
	for _, c := range []string{"bitswap", "dht", "dht/RtRefreshManager", "routing/http/contentrouter", "net/identify", "bs:sess", "bitswap/session", "bitswap_network", "bitswap/network", "bitswap-client", "bitswap/client", "bitswap/client/msgq", "swarm2", "connmgr", "canonical-log"} {
		logging.SetLogLevel(c, "ERROR")
	}

	app := &cli.App{
		Name:  "filexp",
		Usage: "explore filecoin state",
		Commands: []*cli.Command{
			{
				Name:        "get-balance",
				Usage:       "<actor>",
				Description: "Get the balance for a given actor",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(cctx *cli.Context) error {
					actorAddr, err := filaddr.NewFromString(cctx.Args().Get(0))
					if err != nil {
						return err
					}

					bg, ts, err := getAnchorPoint(cctx)
					if err != nil {
						return err
					}
					defer bg.LogStats()

					return getBalance(cctx.Context, bg, ts, actorAddr)
				},
			},
			{
				Name:        "fil-to-eth-address",
				Usage:       "<fX....>",
				Description: "Converts an fX address to a 0x one if possible",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action:      filToEthAddr,
			},
			{
				Name:        "addresses",
				Usage:       "<address>",
				Description: "Lists all the address types associated with an fX or 0x address. Note: will not back calculate fX addresses for f0 or masked ID 0x addresses",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action:      filAddrs,
			},
			{
				Name:        "msig-coins",
				Usage:       "<signer-address>",
				Description: "Add up all of the coins controlled by multisigs with the given signer and signing threshold of 1",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action: func(cctx *cli.Context) error {
					signerAddr, err := filaddr.NewFromString(cctx.Args().Get(0))
					if err != nil {
						return err
					}
					bg, ts, err := getAnchorPoint(cctx)
					if err != nil {
						return err
					}
					defer bg.LogStats()

					return getCoins(cctx.Context, bg, ts, signerAddr)
				},
			},
			{
				Name:        "enumerate-actors",
				Description: "List all actors",
				Flags: append([]cli.Flag{
					&cli.BoolFlag{
						Name:        "count-only",
						Value:       false,
						DefaultText: "will not emit the actor IDs, and just count them",
					},
				}, stateFlags...),
				Action: func(cctx *cli.Context) error {
					bg, ts, err := getAnchorPoint(cctx)
					if err != nil {
						return err
					}
					defer bg.LogStats()

					return getActors(cctx.Context, bg, ts, cctx.Bool("count-only"))
				},
			},
			{
				Name:        "fevm-exec",
				Description: "Execute a read-only FVM actor",
				Usage:       "<eth-addr> <eth-data>",
				Flags: append(append([]cli.Flag{}, stateFlags...),
					&cli.PathFlag{
						Name:  "output",
						Usage: "The path for an output CAR containing the data loaded while computing the result (is not output if undefined)",
					}),
				Action: cmdFevmExec,
			},
			{
				Name:        "fevm-daemon",
				Description: "Start a daemon that will respond to Ethereum JSON RPC calls. Note: passing an RPC endpoint enables real-time updates and lookback-epochs is not supported",
				Usage:       "[port]",
				Flags:       append([]cli.Flag{}, stateFlags...),
				Action:      cmdFevmDaemon,
			},
		},
	}

	topCtx, topCtxShutdown := context.WithCancel(context.Background())

	// sighandler
	go func() {
		sigs := make(chan os.Signal, 1)

		signal.Notify(sigs,
			syscall.SIGTERM,
			syscall.SIGINT,
			syscall.SIGHUP,
			syscall.SIGPIPE,
		)

		// wait
		s := <-sigs

		log.Warnf("process received %s, cleaning up...", decodeSigname(s))

		topCtxShutdown()
	}()
	// end of sighandler

	if err := app.RunContext(topCtx, os.Args); err != nil {
		log.Fatalf("%+v", err)
		os.Exit(1)
	}
}

const chainLoveURL = "https://api.chain.love/"

func getAnchorPoint(cctx *cli.Context) (*blockGetter, *lchtypes.TipSet, error) {
	sourceSelect := []string{"car", "rpc-endpoint", "rpc-fullnode"}

	var countHeadSources int
	for _, s := range sourceSelect {
		if cctx.IsSet(s) {
			countHeadSources++
		}
	}
	if countHeadSources == 0 && cctx.Bool("trust-chainlove") {
		countHeadSources++
		if err := cctx.Set("rpc-endpoint", chainLoveURL); err != nil {
			return nil, nil, err
		}
	}

	if countHeadSources > 1 {
		return nil, nil, xerrors.Errorf(
			"you can not specify more than one CurrentTipsetSource of: %s",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	} else if countHeadSources == 0 && !cctx.IsSet("tipset-cids") {
		return nil, nil, xerrors.Errorf(
			"you have not specified any CurrentTipsetSource (one of %s), as an alternative you must provide the tipset explicitly via '--tipset-cids'",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	}

	rpcAddr := cctx.String("rpc-fullnode")
	if rpcAddr == "" {
		rpcAddr = cctx.String("rpc-endpoint")
	}

	ctx := cctx.Context
	var err error
	var bg *blockGetter
	var tsk *lchtypes.TipSetKey
	var ts *lchtypes.TipSet

	// supplied TSK takes precedence
	if cctx.IsSet("tipset-cids") {
		cidStrs := cctx.StringSlice("tipset-cids")
		var cids []cid.Cid
		for _, s := range cidStrs {
			// urfave is dumb wrt multi-value flags, do some postprocessing
			for _, ss := range regexp.MustCompile(`[\s,:;]`).Split(s, -1) {
				if ss == "" {
					continue
				}
				c, err := cid.Decode(ss)
				if err != nil {
					return nil, nil, err
				}
				cids = append(cids, c)
			}
		}
		tskv := lchtypes.NewTipSetKey(cids...)
		tsk = &tskv
	}

	if cctx.IsSet("car") {
		var carTsk *lchtypes.TipSetKey
		bg, carTsk, err = getStateFromCar(ctx, cctx.String("car"))
		if err != nil {
			return nil, nil, err
		}
		// not forced via --tipset-cids
		if tsk == nil {
			tsk = carTsk
		}
	} else if rpcAddr != "" {
		lApi, apiCloser, err := fil.NewLotusDaemonAPIClientV0(ctx, rpcAddr, 0, "")
		if err != nil {
			return nil, nil, err
		}
		defer apiCloser()

		// not forced via --tipset-cids
		if tsk == nil {
			ts, err = fil.GetTipset(ctx, lApi, filabi.ChainEpoch(cctx.Uint("lookback-epochs")))
			if err != nil {
				return nil, nil, err
			}
		}

		// only a full mode RPC can act as a block source
		if cctx.IsSet("rpc-fullnode") {
			bg = &blockGetter{
				m:              make(map[cid.Cid]int),
				IpldBlockstore: &filRpcBs{lApi},
			}
		}
	}

	// if no block sources available - fall back to public bitswap
	if bg == nil {
		bg, err = initBitswapGetter(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if ts == nil {
		blkData, err := loadBlockData(ctx, bg, tsk.Cids())
		if err != nil {
			return nil, nil, err
		}

		hdrs := make([]*lchtypes.BlockHeader, len(blkData))
		for i := range blkData {
			hdrs[i], err = lchtypes.DecodeBlock(blkData[i])
			if err != nil {
				return nil, nil, err
			}
		}

		ts, err = lchtypes.NewTipSet(hdrs)
		if err != nil {
			return nil, nil, err
		}
	}

	log.Infof("gathering results from StateRoot %s referenced by tipset at height %d (%s) %s", ts.ParentState(), ts.Height(), fil.ClockMainnet.EpochToTime(ts.Height()), ts.Cids())

	return bg, ts, nil
}
