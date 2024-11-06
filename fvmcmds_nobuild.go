//go:build !fvm

package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var errFVMUnsupported error = fmt.Errorf("unsupported in this build - build with fvm tag")

func cmdFevmExec(ctx *cli.Context) error {
	return errFVMUnsupported
}

func cmdFevmDaemon(ctx *cli.Context) error {
	return errFVMUnsupported
}
