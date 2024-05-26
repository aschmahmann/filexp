package main

import (
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lchstate "github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
)

func filToEthAddr(cctx *cli.Context) error {
	args := cctx.Args()
	actorAddrString := args.Get(0)
	addr, err := address.NewFromString(actorAddrString)
	if err != nil {
		return err
	}

	switch p := addr.Protocol(); p {
	case address.ID, address.Delegated:
		eaddr, err := ethtypes.EthAddressFromFilecoinAddress(addr)
		if err != nil {
			return err
		}
		fmt.Println(eaddr.String())
	case address.Actor, address.BLS:
	default:
		return fmt.Errorf("error unsupported address type %v", p)
	}

	bg, ts, err := getAnchorPoint(cctx)
	if err != nil {
		return err
	}
	defer bg.LogStats()

	stateTree, err := lchstate.LoadStateTree(ipldcbor.NewCborStore(bg), ts.ParentState())
	if err != nil {
		return err
	}
	idAddr, err := stateTree.LookupID(addr)
	if err != nil {
		return err
	}

	id, err := address.IDFromAddress(idAddr)
	if err != nil {
		return err
	}
	eaddr := ethtypes.EthAddressFromActorID(abi.ActorID(id))

	fmt.Println(eaddr.String())
	return nil
}
