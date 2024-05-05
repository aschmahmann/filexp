package main

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	carbs "github.com/ipld/go-car/v2/blockstore"

	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"

	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

var hamtOptions = append(filadt.DefaultHamtOptions, hamt.UseTreeBitWidth(filbuiltin.DefaultHamtBitwidth))

func getCoins(ctx context.Context, bg *blockGetter, tsk lchtypes.TipSetKey, addr filaddr.Address) (defErr error) {
	sm, err := newFilStateReader(bg)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
	}

	foundAttoFil := filabi.NewTokenAmount(0)
	if err := parseActors(ctx, sm, ts, addr, foundAttoFil); err != nil {
		return err
	}

	fmt.Printf("total attofil: %s\n", foundAttoFil)
	bg.PrintStats()
	return
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

func getActors(ctx context.Context, bg *blockGetter, tsk lchtypes.TipSetKey, countOnly bool) error {
	sm, err := newFilStateReader(bg)
	if err != nil {
		return xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return xerrors.Errorf("unable to load target tipset: %w", err)
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

func getBalance(ctx context.Context, bg *blockGetter, tsk lchtypes.TipSetKey, addr filaddr.Address) error {
	sm, err := newFilStateReader(bg)
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
	bg.PrintStats()
	return nil
}

func fevmExec(ctx context.Context, bg *blockGetter, ts *lchtypes.TipSet, eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes, outputCAR string) error {
	filMsg, err := getFevmRequest(eaddr, edata)
	if err != nil {
		return err
	}
	fmt.Printf("epoch %s\n", ts.Height())

	sm, err := newFilStateReader(bg)
	if err != nil {
		return err
	}

	act, err := sm.LoadActor(ctx, filMsg.To, ts)
	if err != nil {
		return xerrors.Errorf("could not load actor the message is from: %w", err)
	}
	actorStateRoot := act.Head
	fmt.Printf("actor state root: %s\n", actorStateRoot)

	_, err = bg.Get(ctx, actorStateRoot)
	if err != nil {
		return fmt.Errorf("error loading state root %w", err)
	}

	for _, c := range bg.orderedCids {
		fmt.Printf("pre-call cid: %s\n", c)
	}

	res, err := sm.Call(ctx, filMsg, ts)
	if err != nil {
		return xerrors.Errorf("unable to make a call: %w", err)
	}

	str, err := json.Marshal(res)
	if err != nil {
		fmt.Println(res)
		return xerrors.Errorf("could not marshal response as json: %w", err)
	}

	fmt.Println(string(str))

	bg.PrintStats()

	cidsInOrder := bg.orderedCids
	if err != nil {
		return err
	}

	for _, c := range cidsInOrder {
		fmt.Printf("all cid: %s\n", c)
	}

	if outputCAR != "" {
		carw, err := carbs.OpenReadWrite(outputCAR, []cid.Cid{actorStateRoot}, carbs.WriteAsCarV1(true))
		if err != nil {
			return err
		}

		for _, c := range cidsInOrder {
			blk, err := bg.Get(ctx, c)
			if err != nil {
				return err
			}
			if err := carw.Put(ctx, blk); err != nil {
				return err
			}
		}

		if err := carw.Finalize(); err != nil {
			return err
		}
	}

	return nil
}

func getFevmRequest(eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes) (*lchtypes.Message, error) {
	tx := ethtypes.EthCall{
		From:     nil,
		To:       eaddr,
		Gas:      0,
		GasPrice: ethtypes.EthBigInt{},
		Value:    ethtypes.EthBigInt{},
		Data:     edata,
	}
	filMsg, err := ethCallToFilecoinMessage(tx)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert ethcall to filecoin message: %w", err)
	}

	return filMsg, nil
}

func ethCallToFilecoinMessage(tx ethtypes.EthCall) (*lchtypes.Message, error) {
	var from filaddr.Address
	if tx.From == nil || *tx.From == (ethtypes.EthAddress{}) {
		// Send from the filecoin "system" address.
		var err error
		from, err = (ethtypes.EthAddress{}).ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to construct the ethereum system address: %w", err)
		}
	} else {
		// The from address must be translatable to an f4 address.
		var err error
		from, err = tx.From.ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to translate sender address (%s): %w", tx.From.String(), err)
		}
		if p := from.Protocol(); p != filaddr.Delegated {
			return nil, fmt.Errorf("expected a class 4 address, got: %d: %w", p, err)
		}
	}

	var params []byte
	if len(tx.Data) > 0 {
		initcode := filabi.CborBytes(tx.Data)
		params2, err := actors.SerializeParams(&initcode)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize params: %w", err)
		}
		params = params2
	}

	var to filaddr.Address
	var method filabi.MethodNum
	if tx.To == nil {
		// this is a contract creation
		to = filbuiltin.EthereumAddressManagerActorAddr
		method = filbuiltin.MethodsEAM.CreateExternal
	} else {
		addr, err := tx.To.ToFilecoinAddress()
		if err != nil {
			return nil, xerrors.Errorf("cannot get Filecoin address: %w", err)
		}
		to = addr
		method = filbuiltin.MethodsEVM.InvokeContract
	}

	return &lchtypes.Message{
		From:       from,
		To:         to,
		Value:      filbig.Int(tx.Value),
		Method:     method,
		Params:     params,
		GasLimit:   build.BlockGasLimit,
		GasFeeCap:  filbig.Zero(),
		GasPremium: filbig.Zero(),
	}, nil
}
