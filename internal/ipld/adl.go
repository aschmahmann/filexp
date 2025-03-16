package ipld

import (
	"context"

	filhamt "github.com/filecoin-project/go-hamt-ipld/v3"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/minio/sha256-simd"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var hamtOpts = []filhamt.Option{
	filhamt.UseHashFunction(func(input []byte) []byte {
		res := sha256.Sum256(input)
		return res[:]
	}),
	filhamt.UseTreeBitWidth(filbuiltin.DefaultHamtBitwidth),
}

func ParallelIterateMap(ctx context.Context, cbs ipldcbor.IpldStore, root cid.Cid, cb func(k, vCbor []byte) error) error {
	node, err := filhamt.LoadNode(
		ctx,
		&getManyCborStore{IpldStore: cbs},
		root,
		hamtOpts...,
	)
	if err != nil {
		return err
	}

	return node.ForEachParallel(ctx, func(k string, v *cbg.Deferred) error {
		return cb([]byte(k), v.Raw)
	})
}
