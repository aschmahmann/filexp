package state

import (
	"bytes"
	"context"
	"reflect"
	"strings"

	filexp "github.com/aschmahmann/filexp/internal"
	"github.com/aschmahmann/filexp/internal/ipld"
	filaddr "github.com/filecoin-project/go-address"
	filstore "github.com/filecoin-project/go-state-types/store"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
)

var log = filexp.Logger

func GetActorGeneric(cbs ipldcbor.IpldStore, ts *lchtypes.TipSet, addr filaddr.Address) (*lchtypes.Actor, error) {
	st, err := lchstate.LoadStateTree(cbs, ts.ParentState())
	if err != nil {
		return nil, err
	}
	return st.GetActor(addr)
}

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

type actorState interface {
	GetState() interface{}
}

// all actors mask the Cid slot with a method that returns an unwieldy thing in its place
// some distant day every actor state will have ForEachParallel(), but this is not today
func cidFromStateByFieldpath(as actorState, fieldPath ...string) (cid.Cid, error) {
	s := as.GetState()
	v := reflect.ValueOf(s)

	for _, p := range fieldPath {
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		v = v.FieldByName(p)
		if !v.IsValid() {
			return cid.Undef, xerrors.Errorf("struct %+v, does not seem to contain a fieldPath '%s'", as, strings.Join(fieldPath, "."))
		}
	}
	c, didCast := v.Interface().(cid.Cid)
	if !didCast {
		return cid.Undef, xerrors.Errorf("struct %+v, fieldPath '%s' does not contain a cid.Cid", as, strings.Join(fieldPath, "."))
	}
	return c, nil
}
