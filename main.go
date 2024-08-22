package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
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

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("%+v", err)
		os.Exit(1)
	}
}

const chainLoveURL = "https://api.chain.love/"

func getAnchorPoint(cctx *cli.Context) (*blockGetter, *lchtypes.TipSet, error) {
	sourceSelect := []string{"car", "tipset-cids", "rpc-endpoint"}

	var isSet int
	for _, s := range sourceSelect {
		if cctx.IsSet(s) {
			isSet++
		}
	}
	if isSet == 0 && cctx.Bool("trust-chainlove") {
		isSet++
		if err := cctx.Set("rpc-endpoint", chainLoveURL); err != nil {
			return nil, nil, err
		}
	}

	if isSet != 1 {
		return nil, nil, xerrors.Errorf("you must specify exactly one of: %s", strings.Join(sourceSelect, ", "))
	}

	ctx := cctx.Context
	var err error
	var bg *blockGetter
	var tsk lchtypes.TipSetKey
	var ts *lchtypes.TipSet

	if cctx.IsSet("car") {
		bg, tsk, err = getStateFromCar(ctx, cctx.String("car"))
		if err != nil {
			return nil, nil, err
		}
	} else if cctx.IsSet("tipset-cids") {
		cidStrs := cctx.StringSlice("tipset-cids")
		var cids []cid.Cid
		for _, s := range cidStrs {
			c, err := cid.Decode(s)
			if err != nil {
				return nil, nil, err
			}
			cids = append(cids, c)
		}
		tsk = lchtypes.NewTipSetKey(cids...)
	} else {
		lApi, apiCloser, err := fil.NewLotusDaemonAPIClientV0(ctx, cctx.String("rpc-endpoint"), 0, "")
		if err != nil {
			return nil, nil, err
		}
		defer apiCloser()

		ts, err = fil.GetTipset(ctx, lApi, filabi.ChainEpoch(cctx.Uint("lookback-epochs")))
		if err != nil {
			return nil, nil, err
		}
	}

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
