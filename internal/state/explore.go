package state

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aschmahmann/filexp/internal/ipld"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filstore "github.com/filecoin-project/go-state-types/store"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
)

func GetCoins(ctx context.Context, bg *ipld.CountingBlockGetter, ts *lchtypes.TipSet, addr filaddr.Address) error {
	cbs := ipldcbor.NewCborStore(bg)
	var mx sync.Mutex
	foundAttoFil := filabi.NewTokenAmount(0)

	actorAddrID, err := LookupID(cbs, ts, addr)
	if err != nil {
		return err
	}

	ast := filstore.WrapStore(ctx, cbs)
	if err := IterateActors(ctx, cbs, ts, func(actorID filaddr.Address, act lchtypes.Actor) error {
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

func GetActors(ctx context.Context, bg *ipld.CountingBlockGetter, ts *lchtypes.TipSet, countOnly bool) error {
	var numActors uint64

	if err := IterateActors(ctx, ipldcbor.NewCborStore(bg), ts, func(actorID filaddr.Address, act lchtypes.Actor) error {
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

func GetBalance(_ context.Context, bg *ipld.CountingBlockGetter, ts *lchtypes.TipSet, addr filaddr.Address) error {
	act, err := GetActorGeneric(ipldcbor.NewCborStore(bg), ts, addr)
	if err != nil {
		return err
	}

	fmt.Printf("total actor balance: %s\n", act.Balance)
	return nil
}
