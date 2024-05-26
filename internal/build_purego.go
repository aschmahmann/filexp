//go:build !cgo

package filexp

import (
	// CGO-free sqlite transpile
	_ "modernc.org/sqlite"
)

const DbDriverID = "sqlite"
