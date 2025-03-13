package main

import (
	"fmt"
	"os"
)

func decodeSigname(s os.Signal) string {
	return fmt.Sprintf("signal '%s'", s.String())
}
