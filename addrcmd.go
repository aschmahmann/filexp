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
	case address.SECP256K1, address.Actor, address.BLS:
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
	idAddr, err := stateTree.LookupIDAddress(addr)
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

func filAddrs(cctx *cli.Context) error {
	args := cctx.Args()
	actorAddrString := args.Get(0)

	was0xAddr := false
	var err error
	var eaddr ethtypes.EthAddress
	var addr address.Address
	if actorAddrString[0] == '0' {
		was0xAddr = true
		eaddr, err = ethtypes.ParseEthAddress(actorAddrString)
		if err != nil {
			return err
		}
		addr, err = eaddr.ToFilecoinAddress()
		if err != nil {
			return err
		}
	} else {
		addr, err = address.NewFromString(actorAddrString)
		if err != nil {
			return err
		}
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

	idAddr, err := stateTree.LookupIDAddress(addr)
	if err != nil {
		return err
	}

	id, err := address.IDFromAddress(idAddr)
	if err != nil {
		return err
	}
	eaddr = ethtypes.EthAddressFromActorID(abi.ActorID(id))

	switch p := addr.Protocol(); p {
	case address.SECP256K1, address.Actor, address.BLS:
		// If it's an f1/f2/f3 return the f0, and 0x addresses
		fmt.Println(idAddr)
		fmt.Println(eaddr)
	case address.ID:
		if !was0xAddr {
			// If it's an f0 return the 0x and if enabled the expensive reverse lookup for the fX address
			fmt.Println(eaddr)
		} else {
			// If it's a masked 0x address, return the f0 and if enabled the expensive reverse lookup for the fX address
			fmt.Println(addr)
		}
	case address.Delegated:
		if !was0xAddr {
			fmt.Println(idAddr)
			fmt.Println(eaddr)
		} else {
			fmt.Println(idAddr)
			fmt.Println(addr)
		}
	default:
		return fmt.Errorf("error unsupported address type %v", p)
	}

	return nil
}
