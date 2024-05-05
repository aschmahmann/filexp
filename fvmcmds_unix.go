//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	lchstmgr "github.com/filecoin-project/lotus/chain/stmgr"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
)

func cmdFevmExec(ctx *cli.Context) error {
	args := ctx.Args()
	eaddr, err := ethtypes.ParseEthAddress(args.Get(0))
	if err != nil {
		return xerrors.Errorf("unable to parse eth address: %w", err)
	}
	decodedBytes, err := ethtypes.DecodeHexString(args.Get(1))
	if err != nil {
		return xerrors.Errorf("could not decode hex bytes: %w", err)
	}

	bs, tsk, err := getState(ctx)
	if err != nil {
		return err
	}

	sm, err := newFilStateReader(bs)
	if err != nil {
		return err
	}
	ts, err := sm.ChainStore().GetTipSetFromKey(ctx.Context, tsk)
	if err != nil {
		return err
	}

	return fevmExec(ctx.Context, bs, ts, &eaddr, decodedBytes, ctx.Path("output"))
}

func cmdFevmDaemon(ctx *cli.Context) error {
	args := ctx.Args()
	listenStr := "127.0.0.1:"
	if args.Len() != 0 {
		listenStr += args.First()
	}

	l, err := net.Listen("tcp", listenStr)
	if err != nil {
		return err
	}

	bg, tsk, err := getState(ctx)
	if err != nil {
		return err
	}

	addr := "api.chain.love"
	var api lotusapi.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(ctx.Context, "ws://"+addr+"/rpc/v0", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
	if err != nil {
		return fmt.Errorf("connecting with lotus API failed: %w", err)
	}
	defer closer()

	sm, err := newFilStateReader(bg)
	if err != nil {
		return err
	}
	ts, err := sm.ChainStore().GetTipSetFromKey(ctx.Context, tsk)
	if err != nil {
		return err
	}

	erpc := &ethRpcResolver{
		sm:           sm,
		ts:           ts,
		lastTskCheck: time.Now(),
		api:          api,
	}

	http.HandleFunc("/rpc/v1", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		reqBody, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(err.Error()))
			return
		}

		type jsonRPC struct {
			Jsonrpc string            `json:"jsonrpc"`
			ID      int               `json:"id,omitempty"`
			Method  string            `json:"method"`
			Params  ethtypes.EthCall  `json:"params"`
			Meta    map[string]string `json:"meta,omitempty"`
		}

		reqMsg, err := jsonrpc.DecodeParams[jsonRPC](reqBody)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(err.Error()))
			return
		}
		if reqMsg.Method != "eth_call" {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("only eth_call supported"))
			return
		}

		if reqMsg.Params.To == nil {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("must send to a valid address"))
			return
		}

		if reqMsg.Params.From != nil {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("can only send from null address"))
			return
		}

		eaddr := *reqMsg.Params.To
		methodData := reqMsg.Params.Data

		ret, err := erpc.Call(ctx.Context, eaddr, methodData)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(fmt.Errorf("could not perform call operation: %w", err).Error()))
			return
		}

		type returnType struct {
			Jsonrpc string `json:"jsonrpc"`
			ID      int    `json:"id,omitempty"`
			Result  string `json:"result"`
		}

		sendResponse := returnType{
			Jsonrpc: "2.0",
			ID:      reqMsg.ID,
			Result:  ethtypes.EthBytes(ret).String(),
		}

		responseBytes, err := json.Marshal(sendResponse)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte(xerrors.Errorf("failed to marshal return value: %w", err).Error()))
			return
		}
		_, _ = writer.Write(responseBytes)
	})
	if err := http.Serve(l, nil); err != nil {
		return err
	}
	return nil
}

type ethRpcResolver struct {
	sm           *lchstmgr.StateManager
	ts           *lchtypes.TipSet
	tsMx         sync.RWMutex
	lastTskCheck time.Time
	api          lotusapi.FullNodeStruct
}

func (e *ethRpcResolver) Call(ctx context.Context, eaddr ethtypes.EthAddress, methodData ethtypes.EthBytes) ([]byte, error) {
	e.tsMx.RLock()
	updateTsk := false
	if time.Since(e.lastTskCheck) > time.Second*30 {
		updateTsk = true
	}
	callTS := e.ts
	e.tsMx.RUnlock()

	if updateTsk {
		e.tsMx.Lock()
		if time.Since(e.lastTskCheck) > time.Second*30 {
			ts, err := e.api.ChainHead(ctx)
			if err != nil {
				log.Printf("could not update tipset %s", err.Error())
			} else {
				e.lastTskCheck = time.Now()
				e.ts = ts
			}
		}
		callTS = e.ts
		e.tsMx.Unlock()
	}

	filMsg, err := getFevmRequest(&eaddr, methodData)
	if err != nil {
		return nil, err
	}

	res, err := e.sm.Call(ctx, filMsg, callTS)
	if err != nil {
		return nil, xerrors.Errorf("unable to make a call: %w", err)
	}

	var ret abi.CborBytes
	if err := ret.UnmarshalCBOR(bytes.NewReader(res.MsgRct.Return)); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal return value: %w", err)
	}

	return ret, nil
}
