package eth

import (
	"fmt"

	"golang.org/x/xerrors"

	filaddr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
)

func PrepFevmRequest(eaddr *ethtypes.EthAddress, edata ethtypes.EthBytes) (*lchtypes.Message, error) {
	tx := ethtypes.EthCall{
		From:     nil,
		To:       eaddr,
		Gas:      0,
		GasPrice: ethtypes.EthBigInt{},
		Value:    ethtypes.EthBigInt{},
		Data:     edata,
	}
	filMsg, err := ethCallToFilecoinMessage(tx)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert ethcall to filecoin message: %w", err)
	}

	return filMsg, nil
}

func ethCallToFilecoinMessage(tx ethtypes.EthCall) (*lchtypes.Message, error) {
	var from filaddr.Address
	if tx.From == nil || *tx.From == (ethtypes.EthAddress{}) {
		// Send from the filecoin "system" address.
		var err error
		from, err = (ethtypes.EthAddress{}).ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to construct the ethereum system address: %w", err)
		}
	} else {
		// The from address must be translatable to an f4 address.
		var err error
		from, err = tx.From.ToFilecoinAddress()
		if err != nil {
			return nil, fmt.Errorf("failed to translate sender address (%s): %w", tx.From.String(), err)
		}
		if p := from.Protocol(); p != filaddr.Delegated {
			return nil, fmt.Errorf("expected a class 4 address, got: %d: %w", p, err)
		}
	}

	var params []byte
	if len(tx.Data) > 0 {
		initcode := abi.CborBytes(tx.Data)
		params2, err := actors.SerializeParams(&initcode)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize params: %w", err)
		}
		params = params2
	}

	var to filaddr.Address
	var method abi.MethodNum
	if tx.To == nil {
		// this is a contract creation
		to = filbuiltin.EthereumAddressManagerActorAddr
		method = filbuiltin.MethodsEAM.CreateExternal
	} else {
		addr, err := tx.To.ToFilecoinAddress()
		if err != nil {
			return nil, xerrors.Errorf("cannot get Filecoin address: %w", err)
		}
		to = addr
		method = filbuiltin.MethodsEVM.InvokeContract
	}

	return &lchtypes.Message{
		From:       from,
		To:         to,
		Value:      filbig.Int(tx.Value),
		Method:     method,
		Params:     params,
		GasLimit:   build.BlockGasLimit,
		GasFeeCap:  filbig.Zero(),
		GasPremium: filbig.Zero(),
	}, nil
}
