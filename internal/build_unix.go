//go:build !windows

package filexp

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func DecodeSigname(s os.Signal) string {
	if sysSig, didCast := s.(syscall.Signal); didCast {
		return unix.SignalName(sysSig)
	}

	return fmt.Sprintf("signal '%s'", s.String())
}
