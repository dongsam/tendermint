package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	cfg "github.com/tendermint/tendermint/config"
	tmos "github.com/tendermint/tendermint/libs/os"
	tmrand "github.com/tendermint/tendermint/libs/rand"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
)

// InitFilesCmd initialises a fresh Tendermint Core instance.
var InitFilesCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize Tendermint",
	Args:  cobra.MaximumNArgs(1),
	RunE:  initFiles,
}

func initFiles(cmd *cobra.Command, args []string) error {
	return initFilesWithConfig(config, args)
}

func initFilesWithConfig(config *cfg.Config, args []string) error {
	// tendermint init [validator]
	//validator := len(args) == 1 && args[1] == "validator"
	var pv *privval.FilePV
	// TODO: ADR pv auto gen flag
	if len(args) == 1 && args[0] == cfg.ModeValidator {
		privValKeyFile := config.PrivValidatorKeyFile()
		privValStateFile := config.PrivValidatorStateFile()
		if tmos.FileExists(privValKeyFile) {
			pv = privval.LoadFilePV(privValKeyFile, privValStateFile)
			logger.Info("Found private validator", "keyFile", privValKeyFile,
				"stateFile", privValStateFile)
		} else {
			pv = privval.GenFilePV(privValKeyFile, privValStateFile)
			pv.Save()
			logger.Info("Generated private validator", "keyFile", privValKeyFile,
				"stateFile", privValStateFile)
		}
	}

	nodeKeyFile := config.NodeKeyFile()
	if tmos.FileExists(nodeKeyFile) {
		logger.Info("Found node key", "path", nodeKeyFile)
	} else {
		if _, err := p2p.LoadOrGenNodeKey(nodeKeyFile); err != nil {
			return err
		}
		logger.Info("Generated node key", "path", nodeKeyFile)
	}

	// genesis file
	genFile := config.GenesisFile()
	if tmos.FileExists(genFile) {
		logger.Info("Found genesis file", "path", genFile)
	} else {
		genDoc := types.GenesisDoc{
			ChainID:         fmt.Sprintf("test-chain-%v", tmrand.Str(6)),
			GenesisTime:     tmtime.Now(),
			ConsensusParams: types.DefaultConsensusParams(),
		}
		if pv != nil {
			pubKey, err := pv.GetPubKey()
			if err != nil {
				return fmt.Errorf("can't get pubkey: %w", err)
			}
			genDoc.Validators = []types.GenesisValidator{{
				Address: pubKey.Address(),
				PubKey:  pubKey,
				Power:   10,
			}}
		}
		if err := genDoc.SaveAs(genFile); err != nil {
			return err
		}
		logger.Info("Generated genesis file", "path", genFile)
	}

	return nil
}
