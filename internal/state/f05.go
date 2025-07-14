package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/aschmahmann/filexp/internal/ipld"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lchadt "github.com/filecoin-project/lotus/chain/actors/adt"
	lchmarket "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
)

type deal struct {
	dealID   filabi.DealID
	proposal *lchmarket.DealProposal
}

func DumpStateF05(ctx context.Context, bg ipld.CountingBlockGetter, ts *lchtypes.TipSet, outFh io.Writer, asSingleDocument bool) (defErr error) {

	//
	// Setup various chain access
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

	proposalsReader, err := f05state.Proposals()
	if err != nil {
		return err
	}

	statesReader, err := f05state.States()
	if err != nil {
		return err
	}

	//
	// Setup output
	//
	outBuf := bufio.NewWriterSize(outFh, 1<<20) // 1 MiB buffer, because why not
	defer func() {
		if defErr == nil {
			defErr = outBuf.Flush()
		}
	}()

	if asSingleDocument {
		if _, err := outBuf.WriteString(`{`); err != nil {
			return err
		}
		defer func() {
			_, err := outBuf.WriteString("}\n")
			if defErr == nil {
				defErr = err
			}
		}()
	}

	//
	// Setup emitter workers
	//
	wrkCnt := runtime.NumCPU() - 1
	if wrkCnt < 1 {
		wrkCnt = 1
	} else if wrkCnt > 12 {
		wrkCnt = 12 // do not overwhelm the block provider
	}

	deals := make(chan deal, 16*wrkCnt)
	notFirst := new(bool)

	// Neither stdlib bufio, nor POSIX pipe writes are atomic
	// need a synchronizer either way not to tear the JSON
	mu := new(sync.Mutex)

	// this ctx is not passed to workers: they are shut down on closed input channel alone
	eg, ctx := errgroup.WithContext(ctx)

	for i := wrkCnt; i > 0; i-- {
		eg.Go(func() error { return dealEmitter(asSingleDocument, deals, statesReader, outBuf, mu, notFirst) })
	}

	//
	// Actual chain-reading logic
	//
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
	eg.Go(func() error {
		// if any of the consumers of chan deals errors out, context below is cancelled, so the channeld is closed, so the consumers shut down
		defer close(deals)
		return proposalsReader.ForEach(func(did filabi.DealID, dp lchmarket.DealProposal) error {
			select {
			case <-ctx.Done():
			case deals <- deal{did, &dp}:
			}
			return nil
		})
	})

	return eg.Wait()
}

func dealEmitter(asSingleDoc bool, deals <-chan deal, stateReader lchmarket.DealStates, out *bufio.Writer, mu *sync.Mutex, notFirst *bool) error {

	// inline definition only here for clarity
	// reuse struct across invocations - it has sufficiently low amount of fields that resetting everything is safe/fast
	var toEnc struct {
		DealID   *filabi.DealID `json:",omitempty"`
		Proposal *lchmarket.DealProposal
		State    struct {
			SectorNumber     filabi.SectorNumber
			SectorStartEpoch filabi.ChainEpoch
			LastUpdatedEpoch filabi.ChainEpoch
			SlashEpoch       filabi.ChainEpoch
		}
	}

	jBuf := bytes.NewBuffer(make([]byte, 0, 1024)) // just a starting point, the buffer will self-grow if needed
	jEnc := json.NewEncoder(jBuf)

	for {
		d, isOpen := <-deals
		if !isOpen {
			return nil
		}

		jBuf.Reset()

		if asSingleDoc {
			jBuf.WriteString(fmt.Sprintf(`"%d":`, d.dealID))
		} else {
			toEnc.DealID = &d.dealID
		}

		toEnc.Proposal = d.proposal

		s, found, err := stateReader.Get(d.dealID)
		if err != nil {
			return err
		}
		if found {
			toEnc.State.SectorNumber = s.SectorNumber()
			toEnc.State.SectorStartEpoch = s.SectorStartEpoch()
			toEnc.State.LastUpdatedEpoch = s.LastUpdatedEpoch()
			toEnc.State.SlashEpoch = s.SlashEpoch()
		} else {
			// https://github.com/filecoin-project/lotus/blob/v1.30.0/chain/actors/builtin/market/market.go#L306-L320
			toEnc.State.SectorNumber = 0
			toEnc.State.SectorStartEpoch = -1
			toEnc.State.LastUpdatedEpoch = -1
			toEnc.State.SlashEpoch = -1
		}

		if err := jEnc.Encode(toEnc); err != nil {
			return err
		}

		toWrite := jBuf.Bytes()

		mu.Lock()
		if asSingleDoc {
			// write out the "leading" comma
			if *notFirst {
				_, err = out.WriteRune(',')
			} else {
				*notFirst = true
			}

			// strip the newline inserted by Encode() above
			toWrite = toWrite[:len(toWrite)-1]
		}
		_, err2 := out.Write(toWrite)
		mu.Unlock()

		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
	}
}
