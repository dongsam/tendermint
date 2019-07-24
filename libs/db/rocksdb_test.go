package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	cmn "github.com/tendermint/tendermint/libs/common"
)

func TestRocksDBRocksDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", cmn.RandStr(12))
	defer cleanupDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := NewRocksDB(name, "")
	require.Nil(t, err)
	_, err = NewRocksDB(name, "")
	require.NotNil(t, err)
	wr1.Close() // Close the db to release the lock

	// Test we can open the db twice for reading only
	ro1, err := NewRocksDB(name, "")
	defer ro1.Close()
	require.Nil(t, err)
	ro2, err := NewRocksDB(name, "")
	defer ro2.Close()
	require.Nil(t, err)
}

func BenchmarkRocksDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", cmn.RandStr(12))
	db, err := NewRocksDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}
