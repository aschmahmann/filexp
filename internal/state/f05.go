package state

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"

	"github.com/aschmahmann/filexp/internal/ipld"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lchmarket "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
)

type MarketDealState struct {
	SectorNumber     filabi.SectorNumber
	SectorStartEpoch filabi.ChainEpoch
	LastUpdatedEpoch filabi.ChainEpoch
	SlashEpoch       filabi.ChainEpoch
}
type JsonEntry struct {
	DealID   *filabi.DealID `json:",omitempty"`
	Proposal lchmarket.DealProposal
	State    MarketDealState
}

func DumpStateF05(ctx context.Context, bg *ipld.CountingBlockGetter, ts *lchtypes.TipSet, outFh io.Writer, asSingleDocument bool) error {
	// POSIX pipe writes are not atomic after certain size, we need a synchronizer not to tear the json
	// run the worker in an outer errgroup to allow for all producers to shut down first
	writeSink := make(chan []byte, 8<<10)
	egOuter, ctx := errgroup.WithContext(ctx)
	egOuter.Go(func() error { return writeWorker(ctx, writeSink, outFh, asSingleDocument) })

	egInner, ctx := errgroup.WithContext(ctx)
	wrkCnt := runtime.NumCPU()
	if wrkCnt < 3 {
		wrkCnt = 3 // one iterator, and at least two encoders
	} else if wrkCnt > 12 {
		wrkCnt = 12 // do not overwhelm the block provider
	}
	egInner.SetLimit(wrkCnt)

	//
	// begin actual chain-reading logic
	//
	cbs := ipldcbor.NewCborStore(bg)

	f05act, err := GetActorGeneric(cbs, ts, filbuiltin.StorageMarketActorAddr)
	if err != nil {
		return err
	}
	f05state, err := lchmarket.Load(lchadt.WrapStore(ctx, cbs), f05act)
	if err != nil {
		return err
	}

	proposals, err := f05state.Proposals()
	if err != nil {
		return err
	}

	states, err := f05state.States()
	if err != nil {
		return err
	}

	egInner.Go(func() error {
		var cnt atomic.Int64

		// Currently this takes ~2 minutes for a `return nil` noop via a car file 🪦
		// It should take ~10 seconds instead (based on napking math over tree size, 2.7M blocks)
		//
		// The reason for the discrepancy is lack of a counterpart parallelIterateArray modeled on
		// https://github.com/aschmahmann/filexp/blob/6f5f5d16f7e/internal/ipld/adl.go#L22-L36
		// to then be able to burn through these 2 arrays in parallel and "zip" them up
		// https://github.com/filecoin-project/builtin-actors/blob/v15.0.0/actors/market/src/state.rs#L40-L48
		//
		// Blockers are this PR and its deps: https://github.com/filecoin-project/go-amt-ipld/pull/84
		//
		return proposals.ForEach(func(did filabi.DealID, dp lchmarket.DealProposal) error {

			// keep count, also to deal with trailing comma in case of asSingleDocument
			isFirst := (cnt.Add(1) == 1)

			egInner.Go(func() error {

				// https://github.com/filecoin-project/lotus/blob/v1.30.0/chain/actors/builtin/market/market.go#L306-L320
				mds := MarketDealState{
					SectorNumber:     0,
					SectorStartEpoch: -1,
					LastUpdatedEpoch: -1,
					SlashEpoch:       -1,
				}
				s, found, err := states.Get(did)
				if err != nil {
					return err
				}
				if found {
					mds.SectorNumber = s.SectorNumber()
					mds.SectorStartEpoch = s.SectorStartEpoch()
					mds.LastUpdatedEpoch = s.LastUpdatedEpoch()
					mds.SlashEpoch = s.SlashEpoch()
				}

				// if we do end up with proper concurrency (see comment above), dealing with pooled allocs will be worthwhile
				// right now it's definitively a wash
				toEnc := JsonEntry{
					Proposal: dp,
					State:    mds,
				}
				if !asSingleDocument {
					toEnc.DealID = &did
				}

				enc, err := json.Marshal(toEnc)
				if err != nil {
					return err
				}

				encFin := make([]byte, 0, len(enc)+15)

				if asSingleDocument {
					if !isFirst {
						encFin = append(encFin, ","...)
					}
					encFin = append(encFin, fmt.Sprintf(`"%d":`, did)...)
					encFin = append(encFin, enc...)
				} else {
					encFin = append(encFin, enc...)
					encFin = append(encFin, "\n"...)
				}

				select {
				case <-ctx.Done():
					return nil
				case writeSink <- encFin:
				}

				return nil
			})
			return nil
		})
	})

	innerErr := egInner.Wait()
	close(writeSink)
	outerErr := egOuter.Wait()

	if innerErr != nil {
		return innerErr
	} else if outerErr != nil {
		return outerErr
	}

	return nil
}

func writeWorker(ctx context.Context, in <-chan []byte, out io.Writer, asSingleDocument bool) (defErr error) {

	buf := bufio.NewWriterSize(out, 1<<20)
	defer func() {
		if defErr == nil {
			defErr = buf.Flush()
		}
	}()

	if asSingleDocument {
		if _, err := buf.Write([]byte(`{"id":1,"jsonrpc":"2.0","result":{`)); err != nil {
			return err
		}
		defer func() {
			_, err := buf.Write([]byte("}}\n"))
			if defErr == nil {
				defErr = err
			}
		}()
	}

	for {
		select {
		case b, isOpen := <-in:
			if !isOpen {
				return nil
			}
			if _, err := buf.Write(b); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
