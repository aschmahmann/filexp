package bitswap

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	filexp "github.com/aschmahmann/filexp/internal"
	ipld "github.com/aschmahmann/filexp/internal/ipld"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/verifcid"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
)

var log = filexp.Logger

func InitBitswapGetter(ctx context.Context) (*ipld.CountingBlockGetter, error) {
	start := time.Now()
	defer func() {
		log.Infof("duration to setup bitswap fetching: %v", time.Since(start))
	}()

	h, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		return nil, err
	}

	bscFil, err := setupFilContentFetching(h, ctx)
	if err != nil {
		return nil, err
	}
	bscIpfs, err := setupIPFSContentFetching(h, ctx)
	if err != nil {
		return nil, err
	}

	lds, err := leveldb.NewDatastore("", nil)
	if err != nil {
		return nil, err
	}
	cf := &combineFetcher{
		0, 0,
		bscFil.NewSession(ctx),
		bscIpfs.NewSession(ctx),
	}
	bg := &ipld.CountingBlockGetter{
		IpldBlockstore: &bservWrapper{
			IpldBlockstore: blockstore.NewBlockstore(lds),
			bserv:          cf,
		},
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				peers := h.Network().Peers()
				nPeersWithFilBitswap := 0
				nPeersWithIpfsBitswap := 0
				for _, p := range peers {
					const ipfsBs = "/ipfs/bitswap/1.2.0"
					const filBs = "/chain" + ipfsBs
					retProto, err := h.Peerstore().FirstSupportedProtocol(p, filBs, ipfsBs)
					if err != nil || retProto == "" {
						continue
					}

					//TODO: This isn't technically correct since peers can support both, but in practice none do
					switch retProto {
					case filBs:
						nPeersWithFilBitswap++
					case ipfsBs:
						nPeersWithIpfsBitswap++
					}
				}
				nwantsFil := len(bscFil.GetWantlist())
				nwantsIpfs := len(bscIpfs.GetWantlist())
				nFilBlks := atomic.LoadUint64(&cf.numFirstBlocks)
				nIpfsBlks := atomic.LoadUint64(&cf.numSecondBlocks)
				log.Infow("netStats",
					"peers", len(peers),
					"peersFilBS", nPeersWithFilBitswap,
					"wantsFilBS", nwantsFil,
					"blksFromFilBS", nFilBlks,
					"peersIpfsBS", nPeersWithIpfsBitswap,
					"wantsIpfsBS", nwantsIpfs,
					"blksFromIpfsBS", nIpfsBlks,
					"blksTotal", bg.UniqueBlockCount(),
				)
			}
		}
	}()

	return bg, nil
}

type combineFetcher struct {
	numFirstBlocks, numSecondBlocks uint64
	first, second                   exchange.Fetcher
}

func (f *combineFetcher) GetBlock(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstCh, firstErr := f.first.GetBlocks(subCtx, []cid.Cid{c})
	secondCh, secondErr := f.second.GetBlocks(subCtx, []cid.Cid{c})
	if firstErr != nil && secondErr != nil {
		return nil, multierror.Append(firstErr, secondErr)
	}
	if firstErr != nil {
		blk := <-secondCh
		atomic.AddUint64(&f.numSecondBlocks, 1)
		return blk, nil
	}
	if secondErr != nil {
		blk := <-firstCh
		atomic.AddUint64(&f.numFirstBlocks, 1)
		return blk, nil
	}

	var blk blkfmt.Block
	select {
	case blk = <-firstCh:
		atomic.AddUint64(&f.numFirstBlocks, 1)
	case blk = <-secondCh:
		atomic.AddUint64(&f.numSecondBlocks, 1)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if blk == nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("block missing... this shouldn't happen. Report an error")
	}
	return blk, nil
}

func (f *combineFetcher) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blkfmt.Block, error) {
	//TODO implement me
	panic("implement me")
}

var _ exchange.Fetcher = (*combineFetcher)(nil)

// this is the wrong way around, but this is the easiest way to hack it
type bservWrapper struct {
	ipldcbor.IpldBlockstore
	bserv exchange.Fetcher
}

func (bw *bservWrapper) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	// hash security
	if err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c); err != nil {
		return nil, err
	}

	block, err := bw.IpldBlockstore.Get(ctx, c)
	if err == nil {
		return block, nil
	}

	if ipldfmt.IsNotFound(err) {
		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		blk, err := bw.bserv.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		// also write in the blockstore for caching
		err = bw.IpldBlockstore.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
		return blk, nil
	}

	return nil, err
}

var _ ipldcbor.IpldBlockstore = &bservWrapper{}
