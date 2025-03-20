package ipld

import (
	"context"
	"os"
	"time"

	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	caridx "github.com/ipld/go-car/v2/index"
	"golang.org/x/xerrors"
)

func GetStateFromCar(ctx context.Context, srcSnapshot string) (*CountingBlockGetter, *lchtypes.TipSetKey, error) {
	start := time.Now()

	carbs, err := blockstoreFromSnapshot(srcSnapshot)
	if err != nil {
		return nil, nil, err
	}

	log.Infof("duration to load snapshot: %v", time.Since(start))

	carRoots, err := carbs.Roots()
	if err != nil {
		return nil, nil, err
	}
	tsk := lchtypes.NewTipSetKey(carRoots...)

	// Enable cancelation of tight loops reading from a car file:
	// closing will result in errors from this point on
	go func() {
		<-ctx.Done()
		carbs.Close()
	}()

	return &CountingBlockGetter{IpldBlockstore: carbs}, &tsk, nil
}

func blockstoreFromSnapshot(snapshotFilename string) (*carbs.ReadOnly, error) {
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

		log.Infof("generating new index (slow!!!!) at %s", idxFile)
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
