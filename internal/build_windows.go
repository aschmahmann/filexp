package filexp

import (
	"fmt"
	"os"
)

func DecodeSigname(s os.Signal) string {
	return fmt.Sprintf("signal '%s'", s.String())
}
