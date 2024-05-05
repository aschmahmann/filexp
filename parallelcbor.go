package main

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
)

type getManyCborStore struct {
	*ipldcbor.BasicIpldStore
}

func (g *getManyCborStore) GetMany(ctx context.Context, cids []cid.Cid, outs []interface{}) <-chan *hamt.OptionalInteger {
	outCh := make(chan *hamt.OptionalInteger)
	wg := sync.WaitGroup{}
	processingCh := make(chan int)
	go func() {
		for i := 0; i < len(cids); i++ {
			processingCh <- i
		}
		close(processingCh)
	}()
	const concurrency = 8
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for index := range processingCh {
				c := cids[index]
				o := outs[index]
				err := g.Get(ctx, c, o)
				if err != nil {
					outCh <- &hamt.OptionalInteger{Error: err}
				} else {
					outCh <- &hamt.OptionalInteger{Value: index}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
	return outCh
}

type getManyIPLDStore interface {
	GetMany(ctx context.Context, cids []cid.Cid, outs []interface{}) <-chan *hamt.OptionalInteger
}

var _ getManyIPLDStore = (*getManyCborStore)(nil)
