package core

import (
	"bytes"
	"time"

	tmbytes "github.com/tendermint/tendermint/libs/bytes"
	"github.com/tendermint/tendermint/p2p"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	rpctypes "github.com/tendermint/tendermint/rpc/jsonrpc/types"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/types"
)

// Status returns Tendermint status including node info, pubkey, latest block
// hash, app hash, block height and time.
// More: https://docs.tendermint.com/master/rpc/#/Info/status
func Status(ctx *rpctypes.Context) (*ctypes.ResultStatus, error) {
	var (
		earliestBlockHash     tmbytes.HexBytes
		earliestAppHash       tmbytes.HexBytes
		earliestBlockTimeNano int64

		earliestBlockHeight = env.BlockStore.Base()
	)

	if earliestBlockMeta := env.BlockStore.LoadBlockMeta(earliestBlockHeight); earliestBlockMeta != nil {
		earliestAppHash = earliestBlockMeta.Header.AppHash
		earliestBlockHash = earliestBlockMeta.BlockID.Hash
		earliestBlockTimeNano = earliestBlockMeta.Header.Time.UnixNano()
	}

	var (
		latestBlockHash     tmbytes.HexBytes
		latestAppHash       tmbytes.HexBytes
		latestBlockTimeNano int64

		latestHeight = env.BlockStore.Height()
	)

	if latestHeight != 0 {
		latestBlockMeta := env.BlockStore.LoadBlockMeta(latestHeight)
		if latestBlockMeta != nil {
			latestBlockHash = latestBlockMeta.BlockID.Hash
			latestAppHash = latestBlockMeta.Header.AppHash
			latestBlockTimeNano = latestBlockMeta.Header.Time.UnixNano()
		}
	}

	// Return the very last voting power, not the voting power of this validator
	// during the last block.
	var votingPower int64
//<<<<<<< HEAD
	if val := validatorAtHeight(latestUncommittedHeight()); val != nil {
//=======
	// TODO: ADR Tendermint Mode check latestHeight or latestUncommittedHeight difference
	//if val := validatorAtHeight(latestHeight); val != nil {
//>>>>>>> poc for tendermint mode
		votingPower = val.VotingPower
	}
	validatorInfo := ctypes.ValidatorInfo{}
	if env.PubKey != nil {
		validatorInfo = ctypes.ValidatorInfo{
			Address:     env.PubKey.Address(),
			PubKey:      env.PubKey,
			VotingPower: votingPower,
		}
	}
	result := &ctypes.ResultStatus{
		NodeInfo: env.P2PTransport.NodeInfo().(p2p.DefaultNodeInfo),
		SyncInfo: ctypes.SyncInfo{
			LatestBlockHash:     latestBlockHash,
			LatestAppHash:       latestAppHash,
			LatestBlockHeight:   latestHeight,
			LatestBlockTime:     time.Unix(0, latestBlockTimeNano),
			EarliestBlockHash:   earliestBlockHash,
			EarliestAppHash:     earliestAppHash,
			EarliestBlockHeight: earliestBlockHeight,
			EarliestBlockTime:   time.Unix(0, earliestBlockTimeNano),
			CatchingUp:          env.ConsensusReactor.WaitSync(),
		},
		ValidatorInfo: validatorInfo,
	}

	return result, nil
}

func validatorAtHeight(h int64) *types.Validator {
	valsWithH, err := sm.LoadValidators(env.StateDB, h)
	if err != nil {
		return nil
	}
	if env.PubKey == nil {
		return nil
	}
	privValAddress := env.PubKey.Address()

	// If we're still at height h, search in the current validator set.
	lastBlockHeight, vals := env.ConsensusState.GetValidators()
	if lastBlockHeight == h {
		for _, val := range vals {
			if bytes.Equal(val.Address, privValAddress) {
				return val
			}
		}
	}

	_, val := valsWithH.GetByAddress(privValAddress)
	return val
}
