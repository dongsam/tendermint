package db

import (
	"encoding/binary"
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
	wr1.Close()
	wr2, err := NewRocksDB(name, "")
	require.Nil(t, err)
	wr2.Close() // Close the db to release the lock

	// Test we can open the db twice for reading only
	ro1, err := NewRocksDB(name, "")
	defer ro1.Close()
	require.Nil(t, err)
	ro2, err := NewRocksDB(name, "")
	defer ro2.Close()
	require.Nil(t, err)

	rb := ro1.NewBatch()
	buf := make([]byte, 8)
	buf2 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(10))
	binary.BigEndian.PutUint64(buf2, uint64(20))
	rb.Set(buf, buf)
	rb.Write()
	require.Equal(t, ro1.Has(buf), true)
	require.Equal(t, ro1.Has(buf2), false)
	require.Equal(t, ro1.Get(buf), buf)

	require.Equal(t, ro2.Has(buf), false)
	require.NotEqual(t, ro2.Get(buf), buf)

	ro3, err := NewRocksDB(name, "")
	defer ro3.Close()
	require.Nil(t, err)
	require.Equal(t, ro3.Has(buf), true)
	require.Equal(t, ro3.Has(buf2), false)
	require.Equal(t, ro3.Get(buf), buf)

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
