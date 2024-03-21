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
	"github.com/ipfs/go-ipns"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	rhelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
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

	// setup pubsub for peer discovery so we can do Bitswap
	if err := setupPubSub(ctx, h); err != nil {
		return nil, err
	}

	return bs, nil
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
