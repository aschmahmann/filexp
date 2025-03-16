package filexp

import (
	"fmt"
	"os"

	logging "github.com/ipfs/go-log/v2"
)

var Logger = logging.Logger(fmt.Sprintf("%s(%d)", "filexp", os.Getpid()))
