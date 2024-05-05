package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	lotusbs "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/stmgr"
	chainstore "github.com/filecoin-project/lotus/chain/store"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/cmd/lotus-sim/simulation/mock"
	"github.com/ipfs/boxo/blockstore"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	caridx "github.com/ipld/go-car/v2/index"
	"golang.org/x/xerrors"
)

func newFilStateReader(bs lotusbs.Blockstore) (*stmgr.StateManager, error) {
	mds := dssync.MutexWrap(ds.NewMapDatastore())
	c := cid.MustParse("bafy2bzacecnamqgqmifpluoeldx7zzglxcljo6oja4vrmtj7432rphldpdmm2")
	err := mds.Put(context.TODO(), ds.NewKey("0"), c.Bytes())
	if err != nil {
		return nil, err
	}

	cs := chainstore.NewChainStore(
		bs,
		bs,
		mds,
		nil,
		nil,
	)

	return stmgr.NewStateManager(
		cs,
		consensus.NewTipSetExecutor(filcns.RewardFunc),
		vm.Syscalls(mock.Verifier),
		filcns.DefaultUpgradeSchedule(),
		nil,
		mds,
		nil,
	)
}

func getStateFromCar(ctx context.Context, srcSnapshot string) (blockstore.Blockstore, lchtypes.TipSetKey, error) {
	start := time.Now()
	defer func() {
		fmt.Printf("duration: %v\n", time.Since(start))
	}()

	carbs, err := blockstoreFromSnapshot(ctx, srcSnapshot)
	if err != nil {
		return nil, lchtypes.EmptyTSK, err
	}

	fmt.Printf("duration to load snapshot: %v\n", time.Since(start))

	var tsk lchtypes.TipSetKey
	carRoots, err := carbs.Roots()
	if err != nil {
		return nil, lchtypes.EmptyTSK, err
	}
	tsk = lchtypes.NewTipSetKey(carRoots...)

	return carbs, tsk, nil
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

type countingBlockstore struct {
	blockstore.Blockstore
	mx          sync.Mutex
	m           map[cid.Cid]int
	orderedCids []cid.Cid
}

func (c *countingBlockstore) Get(ctx context.Context, cid cid.Cid) (blkfmt.Block, error) {
	blk, err := c.Blockstore.Get(ctx, cid)
	if err != nil {
		return nil, err
	}

	c.mx.Lock()
	_, found := c.m[cid]
	c.m[cid] = len(blk.RawData())
	if !found {
		c.orderedCids = append(c.orderedCids, cid)
	}
	c.mx.Unlock()

	return blk, nil
}

func (cbs *countingBlockstore) PrintStats() {
	cbs.mx.Lock()
	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes := 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	cbs.mx.Unlock()
	fmt.Printf("total blocks size: %d\n", totalSizeBytes)
}

var _ blockstore.Blockstore = (*countingBlockstore)(nil)
