package ipld

import (
	"context"
	"sync"
	"time"

	filexp "github.com/aschmahmann/filexp/internal"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
)

var log = filexp.Logger

type CountingBlockGetter struct {
	ipldcbor.IpldBlockstore
	mx          sync.Mutex
	firstGet    *time.Time
	m           map[cid.Cid]int
	orderedCids []cid.Cid
}

var _ ipldcbor.IpldBlockstore = &CountingBlockGetter{}

func (bg *CountingBlockGetter) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
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
	}
	bg.mx.Unlock()

	return blk, nil
}

func (bg *CountingBlockGetter) LogStats() {
	bg.mx.Lock()
	totalSizeBytes := 0
	for _, v := range bg.m {
		totalSizeBytes += v
	}
	tsfg := "n/a"
	if bg.firstGet != nil {
		tsfg = time.Since(*bg.firstGet).Truncate(time.Millisecond).String()
	}
	log.Infow("blockgetterStats", "blocksCount", len(bg.m), "blocksBytes", totalSizeBytes, "timeSinceFirstGet", tsfg)
	bg.mx.Unlock()
}

func (bg *CountingBlockGetter) UniqueBlockCount() (cnt int) {
	bg.mx.Lock()
	cnt = len(bg.m)
	bg.mx.Unlock()
	return
}

func (bg *CountingBlockGetter) OrderedCids() []cid.Cid {
	bg.mx.Lock()
	defer bg.mx.Unlock()
	return bg.orderedCids
}
