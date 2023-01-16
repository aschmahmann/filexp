package main

import (
	"context"
	"github.com/filecoin-project/go-address"
	lotusbs "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/stmgr"
	chainstore "github.com/filecoin-project/lotus/chain/store"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	caridx "github.com/ipld/go-car/v2/index"
	"golang.org/x/xerrors"
	"log"
	"os"
)

func cidMustParse(s string) cid.Cid {
	c, err := cid.Parse(s)
	if err != nil {
		panic(err)
	}
	return c
}

func mustAddrID(a address.Address) uint64 {
	i, err := address.IDFromAddress(a)
	if err != nil {
		panic(err)
	}
	return i
}

func newFilStateReader(bs lotusbs.Blockstore) (*stmgr.StateManager, error) {
	return stmgr.NewStateManager(
		chainstore.NewChainStore(
			bs,
			bs,
			ds.NewMapDatastore(),
			nil,
			nil,
		),
		nil,
		nil,
		nil,
		nil,
	)
}

func blockstoreFromSnapshot(ctx context.Context, snapshotFilename string) (*carbs.ReadOnly, error) {
	carFile := snapshotFilename
	carFh, err := os.Open(carFile)
	if err != nil {
		return nil, xerrors.Errorf("unable to open snapshot car at %s: %w", carFile, err)
	}

	var idx caridx.Index
	idxFile := carFile + `.idx`
	idxFh, err := os.Open(idxFile)
	if os.IsNotExist(err) {
		idxFh, err = os.Create(idxFile)
		if err != nil {
			return nil, xerrors.Errorf("unable to create new index %s: %w", idxFile, err)
		}

		log.Printf("generating new index (slow!!!!) at %s", idxFile)
		idx, err = car.GenerateIndex(carFh)
		if err != nil {
			return nil, xerrors.Errorf("car index generation failed: %w", err)
		}
		if _, err := caridx.WriteTo(idx, idxFh); err != nil {
			return nil, xerrors.Errorf("writing out car index to %s failed: %w", idxFile, err)
		}

		// if I do not do this, NewReadOnly will recognize this is an io.reader, and will
		// directly try to read from what is now an EOF :(
		carFh.Seek(0, 0) //nolint:errcheck

	} else if err != nil {
		return nil, xerrors.Errorf("unable to open snapshot index at %s: %w", idxFile, err)
	} else if idx, err = caridx.ReadFrom(idxFh); err != nil {
		return nil, xerrors.Errorf("unable to open preexisting index at %s: %w", idxFile, err)
	}

	roBs, err := carbs.NewReadOnly(carFh, idx)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct blockstore from snapshot and index in %s: %w", carFile, err)
	}

	return roBs, nil
}
