//go:build cgo

package filexp

import (
	_ "github.com/mattn/go-sqlite3"
)

const DbDriverID = "sqlite3"
