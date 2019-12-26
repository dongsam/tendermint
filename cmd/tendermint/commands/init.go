package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	cfg "github.com/tendermint/tendermint/config"
	cmn "github.com/tendermint/tendermint/libs/common"
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

	// private validator
	// TODO: ADR pv auto gen flag
	if len(args) == 1 && args[0] == cfg.ModeValidator {
		privValKeyFile := config.PrivValidatorKeyFile()
		privValStateFile := config.PrivValidatorStateFile()
		if cmn.FileExists(privValKeyFile) {
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
	if cmn.FileExists(nodeKeyFile) {
		logger.Info("Found node key", "path", nodeKeyFile)
	} else {
		if _, err := p2p.LoadOrGenNodeKey(nodeKeyFile); err != nil {
			return err
		}
		logger.Info("Generated node key", "path", nodeKeyFile)
	}

	// genesis file
	genFile := config.GenesisFile()
	if cmn.FileExists(genFile) {
		logger.Info("Found genesis file", "path", genFile)
	} else {
		genDoc := types.GenesisDoc{
			ChainID:         fmt.Sprintf("test-chain-%v", cmn.RandStr(6)),
			GenesisTime:     tmtime.Now(),
			ConsensusParams: types.DefaultConsensusParams(),
		}
		if pv != nil{
			key := pv.GetPubKey()
			genDoc.Validators = []types.GenesisValidator{{
				Address: key.Address(),
				PubKey:  key,
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
