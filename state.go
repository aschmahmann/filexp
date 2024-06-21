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
	_init13 "github.com/filecoin-project/go-state-types/builtin/v13/init"
	filadt "github.com/filecoin-project/go-state-types/builtin/v13/util/adt"
	filstore "github.com/filecoin-project/go-state-types/store"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	lchtypes "github.com/filecoin-project/lotus/chain/types"

	ipldcbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var hamtOptions = append(filadt.DefaultHamtOptions, hamt.UseTreeBitWidth(filbuiltin.DefaultHamtBitwidth))

func getCoins(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, addr filaddr.Address) error {
	foundAttoFil := filabi.NewTokenAmount(0)
	if err := parseActors(ctx, bg, ts, addr, foundAttoFil); err != nil {
		return err
	}

	fmt.Printf("total attofil: %s\n", foundAttoFil)
	return nil
}

func parseActors(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, rootAddr filaddr.Address, foundAttoFil filabi.TokenAmount) error {
	ast := filstore.WrapStore(ctx, ipldcbor.NewCborStore(bg))
	getManyAst := &getManyCborStore{
		BasicIpldStore: ipldcbor.NewCborStore(bg),
	}

	stateTree, err := lchstate.LoadStateTree(ipldcbor.NewCborStore(bg), ts.ParentState())
	if err != nil {
		return err
	}
	rootAddrID, err := stateTree.LookupID(rootAddr)
	if err != nil {
		return err
	}

	var root lchtypes.StateRoot
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(ctx, ts.ParentState(), &root); err != nil {
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

func getActors(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, countOnly bool) error {
	var numActors uint64
	if err := enumActors(ctx, bg, ts, func(actor *lchtypes.Actor, addr filaddr.Address) error {
		atomic.AddUint64(&numActors, 1)
		if !countOnly {
			fmt.Printf("%v\n", addr)
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Printf("total actors found: %d\n", numActors)
	return nil
}

func enumActors(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, actorFunc func(actor *lchtypes.Actor, addr filaddr.Address) error) error {
	ast := filstore.WrapStore(ctx, ipldcbor.NewCborStore(bg))
	getManyAst := &getManyCborStore{
		BasicIpldStore: ipldcbor.NewCborStore(bg),
	}

	var root lchtypes.StateRoot
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(ctx, ts.ParentState(), &root); err != nil {
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

		if err := actorFunc(act, addr); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func enumInit(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, initFunc func(id int64, addr filaddr.Address) error) error {
	ast := filstore.WrapStore(ctx, ipldcbor.NewCborStore(bg))
	getManyAst := &getManyCborStore{
		BasicIpldStore: ipldcbor.NewCborStore(bg),
	}

	var root lchtypes.StateRoot
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(ctx, ts.ParentState(), &root); err != nil {
		return err
	}

	stateTree, err := lchstate.LoadStateTree(ipldcbor.NewCborStore(bg), ts.ParentState())
	if err != nil {
		return err
	}

	initActor, err := stateTree.GetActor(filbuiltin.InitActorAddr)
	if err != nil {
		return err
	}

	var initRoot _init13.State
	// Try loading as a new-style state-tree (version/actors tuple).
	if err := ast.Get(ctx, initActor.Head, &initRoot); err != nil {
		return err
	}

	initHamt, err := hamt.LoadNode(ctx, getManyAst, initRoot.AddressMap, hamtOptions...)
	if err != nil {
		return err
	}

	if err := initHamt.ForEachParallel(ctx, func(k string, val *cbg.Deferred) error {
		var idCbg cbg.CborInt
		addr, err := filaddr.NewFromBytes([]byte(k))
		if err != nil {
			return xerrors.Errorf("invalid address (%x) found in state tree key: %w", []byte(k), err)
		}

		err = idCbg.UnmarshalCBOR(bytes.NewReader(val.Raw))
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := initFunc(int64(idCbg), addr); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func getBalance(_ context.Context, bg *blockGetter, ts *lchtypes.TipSet, addr filaddr.Address) error {
	stateTree, err := lchstate.LoadStateTree(ipldcbor.NewCborStore(bg), ts.ParentState())
	if err != nil {
		return err
	}
	act, err := stateTree.GetActor(addr)
	if err != nil {
		return err
	}

	fmt.Printf("total actor balance: %s\n", act.Balance)
	return nil
}
