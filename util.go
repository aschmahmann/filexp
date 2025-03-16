package main

import (
	"regexp"
	"strings"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	"github.com/aschmahmann/filexp/internal/bitswap"
	ipld "github.com/aschmahmann/filexp/internal/ipld"
	filabi "github.com/filecoin-project/go-state-types/abi"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	// force bundle load, needed for actors.GetActorCodeID() to work
	// DO NOT REMOVE as nothing will work
	_ "github.com/filecoin-project/lotus/build"
)

func stringSliceMap(ss []string, f func(string) string) []string {
	ssout := make([]string, len(ss))
	for i, s := range ss {
		ssout[i] = f(s)
	}
	return ssout
}

func getAnchorPoint(cctx *cli.Context) (*ipld.CountingBlockGetter, *lchtypes.TipSet, error) {
	sourceSelect := []string{"car", "rpc-endpoint", "rpc-fullnode"}

	var countHeadSources int
	for _, s := range sourceSelect {
		if cctx.IsSet(s) {
			countHeadSources++
		}
	}
	if countHeadSources == 0 && cctx.Bool("trust-chainlove") {
		countHeadSources++
		if err := cctx.Set("rpc-endpoint", chainLoveURL); err != nil {
			return nil, nil, err
		}
	}

	if countHeadSources > 1 {
		return nil, nil, xerrors.Errorf(
			"you can not specify more than one CurrentTipsetSource of: %s",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	} else if countHeadSources == 0 && !cctx.IsSet("tipset-cids") {
		return nil, nil, xerrors.Errorf(
			"you have not specified any CurrentTipsetSource (one of %s), as an alternative you must provide the tipset explicitly via '--tipset-cids'",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	}

	rpcAddr := cctx.String("rpc-fullnode")
	if rpcAddr == "" {
		rpcAddr = cctx.String("rpc-endpoint")
	}

	ctx := cctx.Context
	var err error
	var bg *ipld.CountingBlockGetter
	var tsk *lchtypes.TipSetKey
	var ts *lchtypes.TipSet

	// supplied TSK takes precedence
	if cctx.IsSet("tipset-cids") {
		cidStrs := cctx.StringSlice("tipset-cids")
		var cids []cid.Cid
		for _, s := range cidStrs {
			// urfave is dumb wrt multi-value flags, do some postprocessing
			for _, ss := range regexp.MustCompile(`[\s,:;]`).Split(s, -1) {
				if ss == "" {
					continue
				}
				c, err := cid.Decode(ss)
				if err != nil {
					return nil, nil, err
				}
				cids = append(cids, c)
			}
		}
		tskv := lchtypes.NewTipSetKey(cids...)
		tsk = &tskv
	}

	if cctx.IsSet("car") {
		var carTsk *lchtypes.TipSetKey
		bg, carTsk, err = ipld.GetStateFromCar(ctx, cctx.String("car"))
		if err != nil {
			return nil, nil, err
		}
		// not forced via --tipset-cids
		if tsk == nil {
			tsk = carTsk
		}
	} else if rpcAddr != "" {
		lApi, apiCloser, err := fil.NewLotusDaemonAPIClientV0(ctx, rpcAddr, 0, "")
		if err != nil {
			return nil, nil, err
		}
		go func() {
			<-ctx.Done()
			apiCloser()
		}()

		// not forced via --tipset-cids
		if tsk == nil {
			ts, err = fil.GetTipset(ctx, lApi, filabi.ChainEpoch(cctx.Uint("lookback-epochs")))
			if err != nil {
				return nil, nil, err
			}
		}

		// only a full mode RPC can act as a block source
		if cctx.IsSet("rpc-fullnode") {
			bg = &ipld.CountingBlockGetter{IpldBlockstore: &ipld.FilRpcBs{Rpc: lApi}}
		}
	}

	// if no block sources available - fall back to public bitswap
	if bg == nil {
		bg, err = bitswap.InitBitswapGetter(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if ts == nil {
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(8)

		hdrs := make([]*lchtypes.BlockHeader, len(tsk.Cids()))
		for i, c := range tsk.Cids() {
			eg.Go(func() error {
				b, err := bg.Get(ctx, c)
				if err != nil {
					return err
				}
				hdrs[i], err = lchtypes.DecodeBlock(b.RawData())
				return err
			})
		}

		if err = eg.Wait(); err != nil {
			return nil, nil, err
		}

		ts, err = lchtypes.NewTipSet(hdrs)
		if err != nil {
			return nil, nil, err
		}
	}

	log.Infof("gathering results from StateRoot %s referenced by tipset at height %d (%s) %s", ts.ParentState(), ts.Height(), fil.ClockMainnet.EpochToTime(ts.Height()), ts.Cids())

	return bg, ts, nil
}
