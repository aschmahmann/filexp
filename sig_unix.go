//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func decodeSigname(s os.Signal) string {
	if sysSig, didCast := s.(syscall.Signal); didCast {
		return unix.SignalName(sysSig)
	}

	return fmt.Sprintf("signal '%s'", s.String())
}
