package main

import (
	"context"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockstore"
	rc "github.com/ipfs/boxo/routing/http/client"
	contentrouter "github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/host"
)

func setupIPFSContentFetching(h host.Host, ctx context.Context) (*bsclient.Client, error) {
	r, err := rc.New("https://delegated-ipfs.dev", rc.WithStreamResultsRequired(), rc.WithUserAgent("filexp"))
	if err != nil {
		return nil, err
	}

	cr := contentrouter.NewContentRoutingClient(r)
	nullBS := blockstore.NewBlockstore(datastore.NewNullDatastore())
	n := bsnet.NewFromIpfsHost(h, cr)
	bs := bsclient.New(ctx, n, nullBS)
	n.Start(bs)

	return bs, nil
}
