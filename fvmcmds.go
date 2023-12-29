//go:build !unix

package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

func cmdFevmExec(ctx *cli.Context) error {
	return fmt.Errorf("not implemented on Windows")
}

func cmdFevmDaemon(ctx *cli.Context) error {
	return fmt.Errorf("not implemented on Windows")
}
