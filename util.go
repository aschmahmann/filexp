package main

import (
	"bytes"
	"context"
	"os"
	"sync"
	"time"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	filaddr "github.com/filecoin-project/go-address"
	filhamt "github.com/filecoin-project/go-hamt-ipld/v3"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	filstore "github.com/filecoin-project/go-state-types/store"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	caridx "github.com/ipld/go-car/v2/index"
	"github.com/minio/sha256-simd"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	// force bundle load, needed for actors.GetActorCodeID() to work
	// DO NOT REMOVE as nothing will work
	_ "github.com/filecoin-project/lotus/build"
)

func lookupID(cbs *ipldcbor.BasicIpldStore, ts *lchtypes.TipSet, addr filaddr.Address) (filaddr.Address, error) {
	if addr.Protocol() == filaddr.ID {
		return addr, nil
	}
	stateTree, err := lchstate.LoadStateTree(cbs, ts.ParentState())
	if err != nil {
		return filaddr.Undef, err
	}
	return stateTree.LookupIDAddress(addr)
}

func loadStateRoot(ctx context.Context, cbs *ipldcbor.BasicIpldStore, ts *lchtypes.TipSet) (*lchtypes.StateRoot, error) {
	var root lchtypes.StateRoot
	if err := filstore.WrapStore(ctx, cbs).Get(ctx, ts.ParentState(), &root); err != nil {
		return nil, err
	}

	return &root, nil
}

func iterateActors(ctx context.Context, cbs *ipldcbor.BasicIpldStore, ts *lchtypes.TipSet, cb func(actorID filaddr.Address, actor lchtypes.Actor) error) error {
	sr, err := loadStateRoot(ctx, cbs, ts)
	if err != nil {
		return err
	}

	return parallelIterateMap(ctx, cbs, sr.Actors, func(k, vCbor []byte) error {
		id, err := filaddr.NewFromBytes(k)
		if err != nil {
			return xerrors.Errorf("invalid address (%x) found in state tree key: %w", k, err)
		}

		var act lchtypes.Actor
		if err := (&act).UnmarshalCBOR(bytes.NewReader(vCbor)); err != nil {
			return err
		}

		return cb(id, act)
	})
}

var hamtOpts = []filhamt.Option{
	filhamt.UseHashFunction(func(input []byte) []byte {
		res := sha256.Sum256(input)
		return res[:]
	}),
	filhamt.UseTreeBitWidth(filbuiltin.DefaultHamtBitwidth),
}

func parallelIterateMap(ctx context.Context, cbs *ipldcbor.BasicIpldStore, root cid.Cid, cb func(k, vCbor []byte) error) error {
	node, err := filhamt.LoadNode(
		ctx,
		&getManyCborStore{BasicIpldStore: cbs},
		root,
		hamtOpts...,
	)
	if err != nil {
		return err
	}

	return node.ForEachParallel(ctx, func(k string, v *cbg.Deferred) error {
		return cb([]byte(k), v.Raw)
	})
}

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

type filRpcBs struct {
	rpc fil.LotusDaemonAPIClientV0
}

func (frbs *filRpcBs) Put(context.Context, blkfmt.Block) error {
	return xerrors.New("this is a readonly store")
}
func (rbs *filRpcBs) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	d, err := rbs.rpc.ChainReadObj(ctx, c)
	if err != nil {
		return nil, err
	}
	return blkfmt.NewBlockWithCid(d, c)
}
