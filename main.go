package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"golang.org/x/xerrors"
	"log"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/urfave/cli/v2"
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
		addr := "api.chain.love"
		var api lotusapi.FullNodeStruct
		closer, err := jsonrpc.NewMergeClient(context.Background(), "ws://"+addr+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
		if err != nil {
			log.Fatalf("connecting with lotus API failed: %s", err)
		}
		defer closer()

		chainHeightTwoHoursAgo := (time.Now().Add(-2*time.Hour).Unix() - 1598306400) / 30
		tipset, err := api.ChainGetTipSetByHeight(ctx.Context, abi.ChainEpoch(chainHeightTwoHoursAgo), types.TipSetKey{})
		if err != nil {
			log.Fatalf("could not get chain tipset from height %d: %s", chainHeightTwoHoursAgo, err)
		}
		fmt.Printf("using chainheight %d, with reported tipset %s\n", chainHeightTwoHoursAgo, tipset)
		tsk = tipset.Key()
	}

	return getStateDynamicallyLoadedFromBitswap(ctx.Context, tsk)
}
