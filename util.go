package main

import (
	"context"
	"os"
	"sync"
	"time"

	lchtypes "github.com/filecoin-project/lotus/chain/types"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	caridx "github.com/ipld/go-car/v2/index"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func loadBlockData(ctx context.Context, bg *blockGetter, cids []cid.Cid) ([][]byte, error) {
	blks := make([][]byte, len(cids))
	if len(cids) == 0 {
		return blks, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(8)

	for i := range cids {
		i := i
		eg.Go(func() error {
			b, err := bg.Get(ctx, cids[i])
			if err != nil {
				return err
			}
			blks[i] = b.RawData()
			return nil
		})
	}

	return blks, eg.Wait()
}

func getStateFromCar(ctx context.Context, srcSnapshot string) (*blockGetter, lchtypes.TipSetKey, error) {
	start := time.Now()

	carbs, err := blockstoreFromSnapshot(ctx, srcSnapshot)
	if err != nil {
		return nil, lchtypes.EmptyTSK, err
	}

	log.Infof("duration to load snapshot: %v", time.Since(start))

	carRoots, err := carbs.Roots()
	if err != nil {
		return nil, lchtypes.EmptyTSK, err
	}
	tsk := lchtypes.NewTipSetKey(carRoots...)

	return &blockGetter{
		m:              make(map[cid.Cid]int),
		IpldBlockstore: carbs,
	}, tsk, nil
}

func blockstoreFromSnapshot(_ context.Context, snapshotFilename string) (*carbs.ReadOnly, error) {
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

type blockGetter struct {
	ipldcbor.IpldBlockstore
	mx          sync.Mutex
	m           map[cid.Cid]int
	orderedCids []cid.Cid
}

func (bg *blockGetter) Get(ctx context.Context, cid cid.Cid) (blkfmt.Block, error) {
	blk, err := bg.IpldBlockstore.Get(ctx, cid)
	if err != nil {
		return nil, err
	}

	bg.mx.Lock()
	_, found := bg.m[cid]
	bg.m[cid] = len(blk.RawData())
	if !found {
		bg.orderedCids = append(bg.orderedCids, cid)
	}
	bg.mx.Unlock()

	return blk, nil
}

func (bg *blockGetter) LogStats() {
	bg.mx.Lock()
	totalSizeBytes := 0
	for _, v := range bg.m {
		totalSizeBytes += v
	}
	log.Infow("total blocks", "count", len(bg.m), "bytes", totalSizeBytes)
	bg.mx.Unlock()
}

var _ ipldcbor.IpldBlockstore = &blockGetter{}
