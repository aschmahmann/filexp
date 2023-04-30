package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

var boostrappers = []string{
	"/dns4/bootstrap-0.mainnet.filops.net/tcp/1347/p2p/12D3KooWCVe8MmsEMes2FzgTpt9fXtmCY7wrq91GRiaC8PHSCCBj",
	"/dns4/bootstrap-1.mainnet.filops.net/tcp/1347/p2p/12D3KooWCwevHg1yLCvktf2nvLu7L9894mcrJR4MsBCcm4syShVc",
	"/dns4/bootstrap-2.mainnet.filops.net/tcp/1347/p2p/12D3KooWEWVwHGn2yR36gKLozmb4YjDJGerotAPGxmdWZx2nxMC4",
	"/dns4/bootstrap-3.mainnet.filops.net/tcp/1347/p2p/12D3KooWKhgq8c7NQ9iGjbyK7v7phXvG6492HQfiDaGHLHLQjk7R",
	"/dns4/bootstrap-4.mainnet.filops.net/tcp/1347/p2p/12D3KooWL6PsFNPhYftrJzGgF5U18hFoaVhfGk7xwzD8yVrHJ3Uc",
	"/dns4/bootstrap-5.mainnet.filops.net/tcp/1347/p2p/12D3KooWLFynvDQiUpXoHroV1YxKHhPJgysQGH2k3ZGwtWzR4dFH",
	"/dns4/bootstrap-6.mainnet.filops.net/tcp/1347/p2p/12D3KooWP5MwCiqdMETF9ub1P3MbCvQCcfconnYHbWg6sUJcDRQQ",
	"/dns4/bootstrap-7.mainnet.filops.net/tcp/1347/p2p/12D3KooWRs3aY1p3juFjPy8gPN95PEQChm2QKGUCAdcDCC4EBMKf",
	"/dns4/bootstrap-8.mainnet.filops.net/tcp/1347/p2p/12D3KooWScFR7385LTyR4zU1bYdzSiiAb5rnNABfVahPvVSzyTkR",
	"/dns4/lotus-bootstrap.ipfsforce.com/tcp/41778/p2p/12D3KooWGhufNmZHF3sv48aQeS13ng5XVJZ9E6qy2Ms4VzqeUsHk",
	"/dns4/bootstrap-0.starpool.in/tcp/12757/p2p/12D3KooWGHpBMeZbestVEWkfdnC9u7p6uFHXL1n7m1ZBqsEmiUzz",
	"/dns4/bootstrap-1.starpool.in/tcp/12757/p2p/12D3KooWQZrGH1PxSNZPum99M1zNvjNFM33d1AAu5DcvdHptuU7u",
	"/dns4/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
	"/dns4/bootstrap-0.ipfsmain.cn/tcp/34721/p2p/12D3KooWQnwEGNqcM2nAcPtRR9rAX8Hrg4k9kJLCHoTR5chJfz6d",
	"/dns4/bootstrap-1.ipfsmain.cn/tcp/34723/p2p/12D3KooWMKxMkD5DMpSWsW7dBddKxKT7L2GgbNuckz9otxvkvByP",
	"/dns4/bootstarp-0.1475.io/tcp/61256/p2p/12D3KooWRzCVDwHUkgdK7eRgnoXbjDAELhxPErjHzbRLguSV1aRt",
}

func setupContentFetching(ctx context.Context) (host.Host, *bsclient.Client, error) {
	h, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		return nil, nil, err
	}

	nr, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	nullBS := blockstore.NewBlockstore(datastore.NewNullDatastore())
	n := bsnet.NewFromIpfsHost(h, nr, bsnet.Prefix("/chain"))
	bs := bsclient.New(ctx, n, nullBS)
	n.Start(bs)

	// setup pubsub for peer discovery so we can do Bitswap
	if err := setupPubSub(ctx, h); err != nil {
		return nil, nil, err
	}

	return h, bs, nil
}

type hasHost interface {
	Host() host.Host
}

func setupPubSub(ctx context.Context, h host.Host) error {
	// TODO: we're not crawling pubsub particularly well here
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return err
	}
	t, err := ps.Join("/fil/blocks/testnetnet")
	if err != nil {
		return err
	}
	_ = t

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

	wg := sync.WaitGroup{}
	wg.Add(len(boostrappers))
	for _, bstr := range boostrappers {
		ai, err := peer.AddrInfoFromString(bstr)
		if err != nil {
			return err
		}
		go func() {
			_ = h.Connect(ctx, *ai)
			wg.Done()
		}()
	}
	wg.Wait()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

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

	for {
		select {
		case <-ticker.C:
			if np := len(h.Network().Peers()); np > len(boostrappers)*2 {
				return nil
			} else {
				fmt.Printf("number of peers: %d\n", np)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
