package ipld

import (
	"context"
	"fmt"
	"sync"
	"time"

	filexp "github.com/aschmahmann/filexp/internal"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
)

var log = filexp.Logger

type CountingBlockGetter interface {
	ipldcbor.IpldBlockstore
	LogStats()
	TotalBlockCount() int64
	UniqueBlockCount() string
	OrderedCids() []cid.Cid
}

type LiteCBG struct {
	ipldcbor.IpldBlockstore
	mx       sync.Mutex
	firstGet *time.Time
	getCount int64
}

func (bg *LiteCBG) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	blk, err := bg.IpldBlockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	bg.mx.Lock()
	{
		if bg.firstGet == nil {
			t := time.Now()
			bg.firstGet = &t
		}
		bg.getCount++
	}
	bg.mx.Unlock()

	return blk, nil
}

func (bg *LiteCBG) LogStats() {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	tsfg := "n/a"
	if bg.firstGet != nil {
		tsfg = time.Since(*bg.firstGet).Truncate(time.Millisecond).String()
	}
	log.Infow("blockgetterStats", "blocksTotal", bg.getCount, "timeSinceFirstGet", tsfg)
}

func (bg *LiteCBG) TotalBlockCount() int64 {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	return bg.getCount
}

func (bg *LiteCBG) UniqueBlockCount() string { return "n/a" }
func (bg *LiteCBG) OrderedCids() []cid.Cid   { return nil }

type FullCBG struct {
	ipldcbor.IpldBlockstore
	mx          sync.Mutex
	firstGet    *time.Time
	getCount    int64
	m           map[cid.Cid]int
	orderedCids []cid.Cid
}

func (bg *FullCBG) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	blk, err := bg.IpldBlockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	bg.mx.Lock()
	{
		if bg.firstGet == nil {
			bg.m = make(map[cid.Cid]int)
			t := time.Now()
			bg.firstGet = &t
		}
		if _, found := bg.m[c]; !found {
			bg.m[c] = len(blk.RawData())
			bg.orderedCids = append(bg.orderedCids, c)
		}
		bg.getCount++
	}
	bg.mx.Unlock()

	return blk, nil
}

func (bg *FullCBG) LogStats() {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	uniqueSizeBytes := 0
	for _, v := range bg.m {
		uniqueSizeBytes += v
	}
	tsfg := "n/a"
	if bg.firstGet != nil {
		tsfg = time.Since(*bg.firstGet).Truncate(time.Millisecond).String()
	}
	log.Infow("blockgetterStats", "blocksTotal", bg.getCount, "blocksUnique", len(bg.m), "uniqueBytes", uniqueSizeBytes, "timeSinceFirstGet", tsfg)
}

func (bg *FullCBG) TotalBlockCount() int64 {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	return bg.getCount
}

func (bg *FullCBG) UniqueBlockCount() string {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	return fmt.Sprintf("%d", len(bg.m))
}

func (bg *FullCBG) OrderedCids() []cid.Cid {
	bg.mx.Lock()
	defer bg.mx.Unlock()

	return bg.orderedCids
}

func LoadTipset(ctx context.Context, bg CountingBlockGetter, tsk *lchtypes.TipSetKey) (*lchtypes.TipSet, error) {

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(8)

	hdrs := make([]*lchtypes.BlockHeader, len(tsk.Cids()))
	for i, c := range tsk.Cids() {
		eg.Go(func() error {
			b, err := bg.Get(ctx, c)
			if err != nil {
				return err
			}
			hdrs[i], err = lchtypes.DecodeBlock(b.RawData())
			return err
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return lchtypes.NewTipSet(hdrs)
}
