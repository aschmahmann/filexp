package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/big"
	lotusbs "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	leveldb "github.com/ipfs/go-ds-leveldb"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-verifcid"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"sync"
	"sync/atomic"
	"time"

	filaddr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"

	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lbi "github.com/filecoin-project/lotus/chain/actors/builtin"
	lbimsig "github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"

	"github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v10/util/adt"
	cbg "github.com/whyrusleeping/cbor-gen"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipldcbor "github.com/ipfs/go-ipld-cbor"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type countingBlockstore struct {
	blockstore.Blockstore
	mx          sync.Mutex
	m           map[cid.Cid]int
	orderedCids []cid.Cid
}

func (c *countingBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
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

var _ blockstore.Blockstore = (*countingBlockstore)(nil)

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

func getStateDynamicallyLoadedFromBitswap(ctx context.Context) (blockstore.Blockstore, error) {
	start := time.Now()
	defer func() {
		fmt.Printf("duration: %v\n", time.Since(start))
	}()

	h, bsc, err := setupContentFetching(ctx)
	if err != nil {
		return nil, err
	}
	_ = h

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				peers := h.Network().Peers()
				nPeersWithBitswap := 0
				for _, p := range peers {
					retProto, err := h.Peerstore().FirstSupportedProtocol(p, "/chain/ipfs/bitswap/1.2.0")
					if err != nil || retProto == "" {
						continue
					}
					nPeersWithBitswap++
				}
				nwants := len(bsc.GetWantlist())
				fmt.Printf("numPeers: %d, nPeersWithBitswap: %d numWants: %d \n", len(peers), nPeersWithBitswap, nwants)
			}
		}
	}()

	lds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return nil, err
	}
	baseBstore := blockstore.NewBlockstore(lds)
	bs := blockstore.NewIdStore(&bservBstoreWrapper{Blockstore: baseBstore, bserv: bsc.NewSession(ctx)})

	fmt.Printf("duration to setup bitswap fetching: %v\n", time.Since(start))

	return bs, nil
}

// this is the wrong way around, but this is the easiest way to hack it
type bservBstoreWrapper struct {
	blockstore.Blockstore
	bserv exchange.Fetcher
}

func (b *bservBstoreWrapper) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	err := verifcid.ValidateCid(c) // hash security
	if err != nil {
		return nil, err
	}

	block, err := b.Blockstore.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	if ipld.IsNotFound(err) {
		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		blk, err := b.bserv.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		// also write in the blockstore for caching
		err = b.Blockstore.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
		return blk, nil
	}

	return nil, err
}

func (b *bservBstoreWrapper) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	has, err := b.Blockstore.Has(ctx, c)
	if has {
		return b.Blockstore.GetSize(ctx, c)
	}
	if !ipld.IsNotFound(err) {
		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		return 0, err
	}

	blk, err := b.bserv.GetBlock(ctx, c)
	if err != nil {
		return 0, err
	}
	// also write in the blockstore for caching
	err = b.Blockstore.Put(ctx, blk)
	if err != nil {
		return 0, err
	}

	return len(blk.RawData()), nil
}

var _ blockstore.Blockstore = (*bservBstoreWrapper)(nil)

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

type getManyCborStore struct {
	*ipldcbor.BasicIpldStore
}

func (g *getManyCborStore) GetMany(ctx context.Context, cids []cid.Cid, outs []interface{}) <-chan *hamt.OptionalInteger {
	outCh := make(chan *hamt.OptionalInteger)
	wg := sync.WaitGroup{}
	processingCh := make(chan int)
	go func() {
		for i := 0; i < len(cids); i++ {
			processingCh <- i
		}
		close(processingCh)
	}()
	const concurrency = 8
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for index := range processingCh {
				c := cids[index]
				o := outs[index]
				err := g.Get(ctx, c, o)
				if err != nil {
					outCh <- &hamt.OptionalInteger{Error: err}
				} else {
					outCh <- &hamt.OptionalInteger{Value: index}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
	return outCh
}

type getManyIPLDStore interface {
	GetMany(ctx context.Context, cids []cid.Cid, outs []interface{}) <-chan *hamt.OptionalInteger
}

var _ getManyIPLDStore = (*getManyCborStore)(nil)

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

func fevmExec(ctx context.Context, bstore blockstore.Blockstore, tsk lchtypes.TipSetKey, eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes, outputCAR string) error {
	cbs := &countingBlockstore{Blockstore: bstore, m: make(map[cid.Cid]int)}
	ebs := NewEphemeralBlockstore(cbs)
	ts, sm, filMsg, err := getFevmRequest(ctx, ebs, tsk, eaddr, edata)
	if err != nil {
		return err
	}
	fmt.Printf("epoch %s\n", ts.Height())

	act, err := sm.LoadActor(ctx, filMsg.To, ts)
	if err != nil {
		return xerrors.Errorf("could not load actor the message is from: %w", err)
	}
	actorStateRoot := act.Head
	fmt.Printf("actor state root: %s\n", actorStateRoot)

	_, err = ebs.Get(ctx, actorStateRoot)
	if err != nil {
		return fmt.Errorf("error loading state root %w", err)
	}

	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes := 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	fmt.Printf("total block sizes: %d\n", totalSizeBytes)
	for _, c := range cbs.orderedCids {
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

	fmt.Printf("total blocks read: %d\n", len(cbs.m))
	totalSizeBytes = 0
	for _, v := range cbs.m {
		totalSizeBytes += v
	}
	fmt.Printf("total block sizes: %d\n", totalSizeBytes)

	cidsInOrder := cbs.orderedCids
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
			blk, err := bstore.Get(ctx, c)
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

func getFevmRequest(ctx context.Context, ebs lotusbs.Blockstore, tsk lchtypes.TipSetKey, eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes) (*lchtypes.TipSet, *lchstmgr.StateManager, *lchtypes.Message, error) {
	sm, err := newFilStateReader(ebs)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to initialize a StateManager: %w", err)
	}

	ts, err := sm.ChainStore().GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to load target tipset: %w", err)
	}

	tx := ethtypes.EthCall{
		From:     nil,
		To:       eaddr,
		Gas:      0,
		GasPrice: ethtypes.EthBigInt{},
		Value:    ethtypes.EthBigInt{},
		Data:     edata,
	}
	filMsg, err := ethCallToFilecoinMessage(ctx, tx)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("unable to convert ethcall to filecoin message: %w", err)
	}

	return ts, sm, filMsg, nil
}

func ethCallToFilecoinMessage(ctx context.Context, tx ethtypes.EthCall) (*lchtypes.Message, error) {
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
		initcode := abi.CborBytes(tx.Data)
		params2, err := actors.SerializeParams(&initcode)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize params: %w", err)
		}
		params = params2
	}

	var to filaddr.Address
	var method abi.MethodNum
	if tx.To == nil {
		// this is a contract creation
		to = builtin.EthereumAddressManagerActorAddr
		method = builtin.MethodsEAM.CreateExternal
	} else {
		addr, err := tx.To.ToFilecoinAddress()
		if err != nil {
			return nil, xerrors.Errorf("cannot get Filecoin address: %w", err)
		}
		to = addr
		method = builtin.MethodsEVM.InvokeContract
	}

	return &lchtypes.Message{
		From:       from,
		To:         to,
		Value:      big.Int(tx.Value),
		Method:     method,
		Params:     params,
		GasLimit:   build.BlockGasLimit,
		GasFeeCap:  big.Zero(),
		GasPremium: big.Zero(),
	}, nil
}
