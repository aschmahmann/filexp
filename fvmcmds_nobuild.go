//go:build !fvm

package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var fvmUnsupported error = fmt.Errorf("unsupported in this build - build with fvm tag")

func cmdFevmExec(ctx *cli.Context) error {
	return fvmUnsupported
}

func cmdFevmDaemon(ctx *cli.Context) error {
	return fvmUnsupported
}
