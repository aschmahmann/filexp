package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/verifcid"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	rhelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

var boostrappers = []string{
	"/dns4/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
	"/dns4/bootstarp-0.1475.io/tcp/61256/p2p/12D3KooWRzCVDwHUkgdK7eRgnoXbjDAELhxPErjHzbRLguSV1aRt",
	"/dns4/bootstrap-venus.mainnet.filincubator.com/tcp/8888/p2p/QmQu8C6deXwKvJP2D8B6QGyhngc3ZiDnFzEHBDx8yeBXST",
	"/dns4/bootstrap-mainnet-0.chainsafe-fil.io/tcp/34000/p2p/12D3KooWKKkCZbcigsWTEu1cgNetNbZJqeNtysRtFpq7DTqw3eqH",
	"/dns4/bootstrap-mainnet-1.chainsafe-fil.io/tcp/34000/p2p/12D3KooWGnkd9GQKo3apkShQDaq1d6cKJJmsVe6KiQkacUk1T8oZ",
	"/dns4/bootstrap-mainnet-2.chainsafe-fil.io/tcp/34000/p2p/12D3KooWHQRSDFv4FvAjtU32shQ7znz7oRbLBryXzZ9NMK2feyyH",
}

func setupFilContentFetching(h host.Host, ctx context.Context) (*bsclient.Client, error) {
	nullBS := blockstore.NewBlockstore(datastore.NewNullDatastore())
	n := bsnet.NewFromIpfsHost(h, rhelpers.Null{}, bsnet.Prefix("/chain"))
	bs := bsclient.New(ctx, n, nullBS)
	n.Start(bs)

	// setup bootstrap connections
	if err := setupFilBootstrapping(ctx, h); err != nil {
		return nil, err
	}

	// setup pubsub for peer discovery
	if err := setupPubSub(ctx, h); err != nil {
		return nil, err
	}

	// setup dht for peer discovery
	if err := setupFilDHT(ctx, h); err != nil {
		return nil, err
	}

	return bs, nil
}

func setupFilBootstrapping(ctx context.Context, h host.Host) error {
	for _, bstr := range boostrappers {
		ai, err := peer.AddrInfoFromString(bstr)
		if err != nil {
			return err
		}
		go func() {
			_ = h.Connect(ctx, *ai)
		}()
	}
	return nil
}

func setupPubSub(ctx context.Context, h host.Host) error {
	// TODO: we're not crawling pubsub particularly well here
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return err
	}
	_, err = ps.Join("/fil/blocks/testnetnet")
	if err != nil {
		return err
	}
	return nil
}

func setupFilDHT(ctx context.Context, h host.Host) error {
	opts := []dht.Option{dht.Mode(dht.ModeClient),
		dht.Datastore(datastore.NewNullDatastore()),
		dht.Validator(ipns.Validator{}),
		dht.ProtocolPrefix("/fil/kad/testnetnet"),
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(func(d interface{}, p peer.ID) bool {
			allowed := dht.PublicRoutingTableFilter(d, p)
			if !allowed {
				return false
			}
			proto, _ := h.Peerstore().FirstSupportedProtocol(p, "/chain/ipfs/bitswap/1.2.0")
			return proto != ""
		}),
		dht.DisableProviders(),
		dht.DisableValues()}
	d, err := dht.New(ctx, h, opts...)
	if err != nil {
		return err
	}

	go func() {
		t2 := time.NewTicker(time.Second * 10)
		defer t2.Stop()
		select {
		case <-t2.C:
			<-d.ForceRefresh()
		case <-ctx.Done():
			return
		}
	}()

	return nil
}

func initBitswapGetter(ctx context.Context) (*blockGetter, error) {
	start := time.Now()
	defer func() {
		fmt.Printf("duration to setup bitswap fetching: %v\n", time.Since(start))
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
	bg := &blockGetter{
		m: make(map[cid.Cid]int),
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
				bg.mx.Lock()
				fmt.Printf("numPeers: %d, "+
					"nPeersWithFilBitswap: %d, numWantsFil: %d, nblksFil: %d, "+
					"nPeersWithIpfsBitswap: %d, numWantsIpfs: %d, nblksIpfs: %d \n"+
					"numberOfBlocksLoaded: %d\n",
					len(peers), nPeersWithFilBitswap, nwantsFil, nFilBlks, nPeersWithIpfsBitswap, nwantsIpfs, nIpfsBlks, len(bg.m),
				)
				bg.mx.Unlock()

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
