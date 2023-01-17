package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "filexp",
		Usage: "explore filecoin state",
		Commands: []*cli.Command{
			{
				Name:        "msig-coins",
				Usage:       "<signer-key>",
				Description: "Add up all of the coins controlled by multisigs with the given signer and signing threshold of 1. Either pass a state CAR, tipset CIDs, or trust chain.love for a recent time",
				Flags: []cli.Flag{
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
				},
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					signerKey := args.Get(0)
					signerAddr, err := address.NewFromString(signerKey)
					if err != nil {
						return err
					}

					carSet := ctx.IsSet("car")
					tsSet := ctx.IsSet("tipset-cids")
					clSet := ctx.IsSet("trust-chainlove")

					if carSet {
						carLocation := ctx.String("car")
						if tsSet || clSet {
							return fmt.Errorf("choose only one of CAR, tipset-cids, or trust-chainlove")
						}
						return getCoinsFromCar(ctx.Context, carLocation, signerAddr)
					}

					var tsk types.TipSetKey

					if tsSet {
						if clSet {
							return fmt.Errorf("choose only one of CAR, tipset-cids, or trust-chainlove")
						}
						cidStrs := ctx.StringSlice("tipset-cids")
						var cids []cid.Cid
						for _, s := range cidStrs {
							c, err := cid.Decode(s)
							if err != nil {
								return err
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

					return getCoinsFromBitswap(ctx.Context, tsk, signerAddr)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
