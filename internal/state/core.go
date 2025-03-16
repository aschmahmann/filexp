package state

import (
	"bytes"
	"context"

	"github.com/aschmahmann/filexp/internal/ipld"
	filaddr "github.com/filecoin-project/go-address"
	filstore "github.com/filecoin-project/go-state-types/store"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
)

func LookupID(cbs ipldcbor.IpldStore, ts *lchtypes.TipSet, addr filaddr.Address) (filaddr.Address, error) {
	if addr.Protocol() == filaddr.ID {
		return addr, nil
	}
	stateTree, err := lchstate.LoadStateTree(cbs, ts.ParentState())
	if err != nil {
		return filaddr.Undef, err
	}
	return stateTree.LookupIDAddress(addr)
}

func LoadStateRoot(ctx context.Context, cbs ipldcbor.IpldStore, ts *lchtypes.TipSet) (*lchtypes.StateRoot, error) {
	var root lchtypes.StateRoot
	if err := filstore.WrapStore(ctx, cbs).Get(ctx, ts.ParentState(), &root); err != nil {
		return nil, err
	}

	return &root, nil
}

func IterateActors(ctx context.Context, cbs ipldcbor.IpldStore, ts *lchtypes.TipSet, cb func(actorID filaddr.Address, actor lchtypes.Actor) error) error {
	sr, err := LoadStateRoot(ctx, cbs, ts)
	if err != nil {
		return err
	}

	return ipld.ParallelIterateMap(ctx, cbs, sr.Actors, func(k, vCbor []byte) error {
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
