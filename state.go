package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	filaddr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v10/util/adt"
	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func getCoins(ctx context.Context, bstore blockstore.Blockstore, tsk lchtypes.TipSetKey, addr filaddr.Address) (defErr error) {
	cbs := &countingBlockstore{Blockstore: bstore, m: make(map[cid.Cid]int)}
	ebs := NewEphemeralBlockstore(cbs)
	sm, err := newFilStateReader(ebs)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
	}

	eg, shCtx := errgroup.WithContext(ctx)

	foundAttoFil := abi.NewTokenAmount(0)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shCtx.Done():
				return
			case <-ticker.C:
				cbs.mx.Lock()
				nb := len(cbs.m)
				cbs.mx.Unlock()
				fmt.Printf("numberOfBlocksLoaded: %d \n", nb)
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
	getManyAst := &getManyCborStore{
		BasicIpldStore: ipldcbor.NewCborStore(sm.ChainStore().StateBlockstore())}

	stateTree, err := sm.StateTree(ts.ParentState())
	if err != nil {
		return err
	}
	rootAddrID, err := stateTree.LookupID(rootAddr)
	if err != nil {
		return err
	}

	var root lchtypes.StateRoot
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(context.TODO(), ts.ParentState(), &root); err != nil {
		return err
	}

	hamtOptions := append(adt.DefaultHamtOptions, hamt.UseTreeBitWidth(builtin.DefaultHamtBitwidth))
	node, err := hamt.LoadNode(ctx, getManyAst, root.Actors, hamtOptions...)
	if err != nil {
		return err
	}

	var mx sync.Mutex
	return node.ForEachParallel(ctx, func(k string, val *cbg.Deferred) error {
		act := &lchtypes.Actor{}
		addr, err := filaddr.NewFromBytes([]byte(k))
		if err != nil {
			return xerrors.Errorf("invalid address (%x) found in state tree key: %w", []byte(k), err)
		}

		err = act.UnmarshalCBOR(bytes.NewReader(val.Raw))
		if err != nil {
			return err
		}

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

func getActors(ctx context.Context, bstore blockstore.Blockstore, tsk lchtypes.TipSetKey, countOnly bool) error {
	cbs := &countingBlockstore{Blockstore: bstore, m: make(map[cid.Cid]int)}
	ebs := NewEphemeralBlockstore(cbs)
	sm, err := newFilStateReader(ebs)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
	}

	eg, shCtx := errgroup.WithContext(ctx)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shCtx.Done():
				return
			case <-ticker.C:
				cbs.mx.Lock()
				nb := len(cbs.m)
				cbs.mx.Unlock()
				fmt.Printf("numberOfBlocksLoaded: %d \n", nb)
			}
		}
	}()

	var numActors uint64

	eg.Go(func() error {
		ast := lchadt.WrapStore(ctx, ipldcbor.NewCborStore(sm.ChainStore().UnionStore()))
		getManyAst := &getManyCborStore{
			BasicIpldStore: ipldcbor.NewCborStore(sm.ChainStore().StateBlockstore())}

		var root lchtypes.StateRoot
		// Try loading as a new-style state-tree (version/actors tuple).
		if err := ast.Get(context.TODO(), ts.ParentState(), &root); err != nil {
			return err
		}

		hamtOptions := append(adt.DefaultHamtOptions, hamt.UseTreeBitWidth(builtin.DefaultHamtBitwidth))
		node, err := hamt.LoadNode(ctx, getManyAst, root.Actors, hamtOptions...)
		if err != nil {
			return err
		}

		return node.ForEachParallel(ctx, func(k string, val *cbg.Deferred) error {
			act := &lchtypes.Actor{}
			addr, err := filaddr.NewFromBytes([]byte(k))
			if err != nil {
				return xerrors.Errorf("invalid address (%x) found in state tree key: %w", []byte(k), err)
			}

			err = act.UnmarshalCBOR(bytes.NewReader(val.Raw))
			if err != nil {
				return err
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}
			atomic.AddUint64(&numActors, 1)
			if !countOnly {
				fmt.Printf("%v\n", addr)
			}
			return nil
		})
	})
	err = eg.Wait()
	if err != nil {
		return err
	}

	fmt.Printf("total actors found: %d\n", numActors)
	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes := 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	fmt.Printf("total block sizes: %d\n", totalSizeBytes)
	return nil
}

func getBalance(ctx context.Context, bstore blockstore.Blockstore, tsk lchtypes.TipSetKey, addr filaddr.Address) error {
	cbs := &countingBlockstore{Blockstore: bstore, m: make(map[cid.Cid]int)}
	ebs := NewEphemeralBlockstore(cbs)
	sm, err := newFilStateReader(ebs)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
	}

	stateTree, err := sm.StateTree(ts.ParentState())
	if err != nil {
		return err
	}
	act, err := stateTree.GetActor(addr)
	if err != nil {
		return err
	}

	fmt.Printf("total actor balance: %s\n", act.Balance)
	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes := 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	fmt.Printf("total block sizes: %d\n", totalSizeBytes)
	return nil
}
