package bitswap

import (
	"context"
	"time"

	lbuild "github.com/filecoin-project/lotus/build"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-datastore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	rhelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func setupFilContentFetching(h host.Host, ctx context.Context) (*bsclient.Client, error) {
	nullBS := blockstore.NewBlockstore(datastore.NewNullDatastore())
	n := bsnet.NewFromIpfsHost(h, bsnet.Prefix("/chain"))
	bs := bsclient.New(ctx, n, rhelpers.Null{}, nullBS)
	n.Start(bs)

	// setup bootstrap connections
	if err := setupFilBootstrapping(ctx, h); err != nil {
		return nil, err
	}

	// setup pubsub for peer discovery
	if err := setupFilPubSub(ctx, h); err != nil {
		return nil, err
	}

	// setup dht for peer discovery
	if err := setupFilDHT(ctx, h); err != nil {
		return nil, err
	}

	return bs, nil
}

func setupFilBootstrapping(ctx context.Context, h host.Host) error {
	bsList, err := lbuild.BuiltinBootstrap()
	if err != nil {
		return err
	}

	for _, ai := range bsList {
		go func() {
			_ = h.Connect(ctx, ai)
		}()
	}
	return nil
}

func setupFilPubSub(ctx context.Context, h host.Host) error {
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
