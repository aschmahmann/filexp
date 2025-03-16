package ipld

import (
	"context"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

type FilRpcBs struct {
	Rpc fil.LotusDaemonAPIClientV0
}

func (frbs *FilRpcBs) Put(context.Context, blkfmt.Block) error {
	return xerrors.New("this is a readonly store")
}
func (rbs *FilRpcBs) Get(ctx context.Context, c cid.Cid) (blkfmt.Block, error) {
	d, err := rbs.Rpc.ChainReadObj(ctx, c)
	if err != nil {
		return nil, err
	}
	return blkfmt.NewBlockWithCid(d, c)
}
