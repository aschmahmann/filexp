package main

import (
	"context"

	lotusbs "github.com/filecoin-project/lotus/blockstore"
	ipfsbs "github.com/ipfs/boxo/blockstore"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"golang.org/x/xerrors"
)

// copied from github.com/ribasushi/fil-fip36-vote-tally

func NewEphemeralBlockstore(wrapped ipfsbs.Blockstore) lotusbs.Blockstore {
	return lotusbs.NewIDStore(&ephbs{
		ramBs:     lotusbs.FromDatastore(dssync.MutexWrap(ds.NewMapDatastore())),
		wrappedBs: wrapped,
	})
}

type ephbs struct {
	ramBs     lotusbs.Blockstore
	wrappedBs ipfsbs.Blockstore
}

var _ = lotusbs.Blockstore(&ephbs{})

// don't bother
func (e *ephbs) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	return nil, xerrors.Errorf("method AllKeysChan is not supported ")
}
func (e *ephbs) DeleteMany(context.Context, []cid.Cid) error {
	return xerrors.Errorf("method DeleteMany is not supported ")
}
func (e *ephbs) DeleteBlock(context.Context, cid.Cid) error {
	return xerrors.Errorf("method DeleteBlock is not supported ")
}

// whatever
func (e *ephbs) HashOnRead(enable bool) { e.wrappedBs.HashOnRead(enable) }

// implement the rest properly
func (e *ephbs) Put(ctx context.Context, blk blkfmt.Block) error { return e.ramBs.Put(ctx, blk) }

func (e *ephbs) PutMany(ctx context.Context, blks []blkfmt.Block) error {
	return e.ramBs.PutMany(ctx, blks)
}

func (e *ephbs) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ramHas, err := e.ramBs.Has(ctx, c)
	switch {

	case err != nil:
		return false, err

	case ramHas:
		return ramHas, nil

	default:
		return e.wrappedBs.Has(ctx, c)
	}
}

func (e *ephbs) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	ramHas, err := e.ramBs.Has(ctx, c)
	switch {

	case err != nil:
		return err

	case ramHas:
		return e.ramBs.View(ctx, c, callback)

	default:
		b, err := e.wrappedBs.Get(ctx, c)
		if err != nil {
			return err
		}
		return callback(b.RawData())
	}
}

func (e *ephbs) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ramHas, err := e.ramBs.Has(ctx, c)
	switch {

	case err != nil:
		return -1, err

	case ramHas:
		return e.ramBs.GetSize(ctx, c)

	default:
		return e.wrappedBs.GetSize(ctx, c)
	}
}

func (e *ephbs) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	ramHas, err := e.ramBs.Has(ctx, c)
	switch {

	case err != nil:
		return nil, err

	case ramHas:
		return e.ramBs.Get(ctx, c)

	default:
		return e.wrappedBs.Get(ctx, c)
	}
}

func (e *ephbs) Flush(ctx context.Context) error {
	return nil
}
