package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	filaddr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"

	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipldcbor "github.com/ipfs/go-ipld-cbor"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type countingBlockstore struct {
	blockstore.Blockstore
	mx sync.Mutex
	m  map[cid.Cid]int
}

func (c *countingBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	blk, err := c.Blockstore.Get(ctx, cid)
	if err != nil {
		return nil, err
	}

	c.mx.Lock()
	c.m[cid] = len(blk.RawData())
	c.mx.Unlock()

	return blk, nil
}

var _ blockstore.Blockstore = (*countingBlockstore)(nil)

func getCoins(ctx context.Context, srcSnapshot string, addr filaddr.Address) (defErr error) {
	carbs, err := blockstoreFromSnapshot(ctx, srcSnapshot)
	if err != nil {
		return err
	}

	var tsk lchtypes.TipSetKey
	carRoots, err := carbs.Roots()
	if err != nil {
		return err
	}
	tsk = lchtypes.NewTipSetKey(carRoots...)

	cbs := &countingBlockstore{Blockstore: carbs, m: make(map[cid.Cid]int)}
	sm, err := newFilStateReader(cbs)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
	}

	eg, shCtx := errgroup.WithContext(ctx)

	foundAttoFil := abi.NewTokenAmount(0)

	printStats := func() {
		os.Stderr.WriteString(fmt.Sprintf( //nolint:errcheck
			"Found attofil: % 5d\r", foundAttoFil.Uint64(),
		),
		)
	}

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shCtx.Done():
				printStats()
				os.Stderr.WriteString("\n") //nolint:errcheck
				return
			case <-ticker.C:
				printStats()
			}
		}
	}()

	eg.Go(func() error { return parseActors(shCtx, sm, ts, addr, foundAttoFil) })
	err = eg.Wait()
	if err != nil {
		return err
	}

	fmt.Printf("total attofil: %s\n", foundAttoFil)
	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes := 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	fmt.Printf("total block sizes: %d\n", totalSizeBytes)
	return
}

func parseActors(ctx context.Context, sm *lchstmgr.StateManager, ts *lchtypes.TipSet, rootAddr filaddr.Address, foundAttoFil abi.TokenAmount) error {
	ast := lchadt.WrapStore(ctx, ipldcbor.NewCborStore(sm.ChainStore().UnionStore()))

	stateTree, err := sm.StateTree(ts.ParentState())
	if err != nil {
		return err
	}
	rootAddrID, err := stateTree.LookupID(rootAddr)
	if err != nil {
		return err
	}

	var mx sync.Mutex
	return stateTree.ForEach(func(addr filaddr.Address, act *lchtypes.Actor) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case lbi.IsMultisigActor(act.Code):
			ms, err := lbimsig.Load(ast, act)
			if err != nil {
				return err
			}
			msID := mustAddrID(addr)
			_ = msID
			tr, _ := ms.Threshold()

			actors, err := ms.Signers()
			if err != nil {
				return err
			}

			foundActor := false
			for _, a := range actors {
				if bytes.Equal(a.Bytes(), rootAddrID.Bytes()) {
					foundActor = true
				}
			}
			if foundActor && tr == 1 {
				// msig balance needs calculating for epoch in question
				lb, err := ms.LockedBalance(ts.Height())
				if err != nil {
					return err
				}

				bal := filbig.Sub(act.Balance, lb)

				mx.Lock()
				foundAttoFil.Add(foundAttoFil.Int, bal.Int)
				mx.Unlock()
				fmt.Printf("found msig %v\n", addr)
			} else if foundActor {
				fmt.Printf("more than just you is needed to claim the coins\n")
			}
			return err
		default:
			return nil
		}
	})
}
