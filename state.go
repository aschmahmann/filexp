package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	filaddr "github.com/filecoin-project/go-address"
	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	filadt "github.com/filecoin-project/go-state-types/builtin/v13/util/adt"
	filstore "github.com/filecoin-project/go-state-types/store"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var hamtOptions = append(filadt.DefaultHamtOptions, hamt.UseTreeBitWidth(filbuiltin.DefaultHamtBitwidth))

func getCoins(ctx context.Context, bg *blockGetter, ts *fil.LotusTS, addr filaddr.Address) error {
	sm, err := newFilStateReader(bg)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	foundAttoFil := filabi.NewTokenAmount(0)
	if err := parseActors(ctx, sm, ts, addr, foundAttoFil); err != nil {
		return err
	}

	fmt.Printf("total attofil: %s\n", foundAttoFil)
	bg.PrintStats()
	return nil
}

func parseActors(ctx context.Context, sm *lchstmgr.StateManager, ts *lchtypes.TipSet, rootAddr filaddr.Address, foundAttoFil filabi.TokenAmount) error {
	ast := filstore.WrapStore(ctx, ipldcbor.NewCborStore(sm.ChainStore().UnionStore()))
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

func getActors(ctx context.Context, bg *blockGetter, ts *fil.LotusTS, countOnly bool) error {
	sm, err := newFilStateReader(bg)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	var numActors uint64

	ast := filstore.WrapStore(ctx, ipldcbor.NewCborStore(sm.ChainStore().UnionStore()))
	getManyAst := &getManyCborStore{
		BasicIpldStore: ipldcbor.NewCborStore(sm.ChainStore().StateBlockstore())}

	var root lchtypes.StateRoot
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(context.TODO(), ts.ParentState(), &root); err != nil {
		return err
	}

	node, err := hamt.LoadNode(ctx, getManyAst, root.Actors, hamtOptions...)
	if err != nil {
		return err
	}

	if err := node.ForEachParallel(ctx, func(k string, val *cbg.Deferred) error {
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
	}); err != nil {
		return err
	}

	fmt.Printf("total actors found: %d\n", numActors)
	bg.PrintStats()
	return nil
}

func getBalance(ctx context.Context, bg *blockGetter, ts *fil.LotusTS, addr filaddr.Address) error {
	sm, err := newFilStateReader(bg)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
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
	bg.PrintStats()
	return nil
}
