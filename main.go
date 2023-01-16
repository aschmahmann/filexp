package main

import (
	"github.com/filecoin-project/go-address"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "filexp",
		Usage: "explore filecoin state",
		Commands: []*cli.Command{
			{
				Name:  "get-coins",
				Usage: "<state CAR> <root-key>",
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					carLocation := args.Get(0)
					rootKey := args.Get(1)
					rootAddr, err := address.NewFromString(rootKey)
					if err != nil {
						return err
					}

					return getCoins(ctx.Context, carLocation, rootAddr)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
