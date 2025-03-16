package ipld

import (
	"context"

	lbs "github.com/filecoin-project/lotus/blockstore"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type FallbackBs struct {
	lbs.Blockstore
	FallbackGet func(ctx context.Context, c cid.Cid) (blkfmt.Block, error)
}

func (fbs FallbackBs) maybeFallback(ctx context.Context, c cid.Cid) error {
	if alreadyCanHaz, _ := fbs.Blockstore.Has(ctx, c); alreadyCanHaz {
		return nil
	}

	b, err := fbs.FallbackGet(ctx, c)
	if err != nil {
		return err
	}

	return fbs.Put(ctx, b)
}

func (fbs FallbackBs) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	if err := fbs.maybeFallback(ctx, c); err != nil {
		return nil, err
	}
	return fbs.Blockstore.Get(ctx, c)
}

func (fbs FallbackBs) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if err := fbs.maybeFallback(ctx, c); err != nil {
		return false, err
	}
	return fbs.Blockstore.Has(ctx, c)
}

func (fbs FallbackBs) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if err := fbs.maybeFallback(ctx, c); err != nil {
		return -1, err
	}
	return fbs.Blockstore.GetSize(ctx, c)
}

func (fbs FallbackBs) View(ctx context.Context, c cid.Cid, cb func([]byte) error) error {
	if err := fbs.maybeFallback(ctx, c); err != nil {
		return err
	}
	return fbs.Blockstore.View(ctx, c, cb)
}
