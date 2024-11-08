package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filstore "github.com/filecoin-project/go-state-types/store"

	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	lchtypes "github.com/filecoin-project/lotus/chain/types"

	ipldcbor "github.com/ipfs/go-ipld-cbor"
)

func getCoins(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, addr filaddr.Address) error {
	cbs := ipldcbor.NewCborStore(bg)
	var mx sync.Mutex
	foundAttoFil := filabi.NewTokenAmount(0)

	actorAddrID, err := lookupID(cbs, ts, addr)
	if err != nil {
		return err
	}

	ast := filstore.WrapStore(ctx, cbs)
	if err := iterateActors(ctx, cbs, ts, func(actorID filaddr.Address, act lchtypes.Actor) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case lbi.IsMultisigActor(act.Code):
			ms, err := lbimsig.Load(ast, &act)
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
				if bytes.Equal(a.Bytes(), actorAddrID.Bytes()) {
					foundActor = true
					break
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
				fmt.Printf("found msig %v\n", actorID)
			} else if foundActor {
				fmt.Printf("found msig %v: more than just you is needed to claim the coins\n", actorID)
			}
			return err
		default:
			return nil
		}
	}); err != nil {
		return err
	}

	fmt.Printf("total attofil: %s\n", foundAttoFil)
	return nil
}

func getActors(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, countOnly bool) error {
	var numActors uint64

	if err := iterateActors(ctx, ipldcbor.NewCborStore(bg), ts, func(actorID filaddr.Address, act lchtypes.Actor) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		atomic.AddUint64(&numActors, 1)
		if !countOnly {
			fmt.Printf("%v\n", actorID)
		}
		return nil
	}); err != nil {
		return err
	}

	fmt.Printf("total actors found: %d\n", numActors)
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
