package bitswap

import (
	"context"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
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

	nullBS := blockstore.NewBlockstore(datastore.NewNullDatastore())
	n := bsnet.NewFromIpfsHost(h)
	bs := bsclient.New(ctx, n, contentrouter.NewContentRoutingClient(r), nullBS)
	n.Start(bs)

	return bs, nil
}
