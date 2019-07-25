package db

import (
	"bytes"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"path/filepath"

	//"github.com/syndtr/goleveldb/leveldb"
	//"github.com/syndtr/goleveldb/leveldb/errors"
	//"github.com/syndtr/goleveldb/leveldb/iterator"
	//"github.com/syndtr/goleveldb/leveldb/opt"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewRocksDB(name, dir)
	}
	registerDBCreator(RocksDBBackend, dbCreator, false)
}

var _ DB = (*RocksDB)(nil)

type RocksDB struct {
	db                  *gorocksdb.DB
	ro                  *gorocksdb.ReadOptions
	wo                  *gorocksdb.WriteOptions
	columnFamilyHandles gorocksdb.ColumnFamilyHandles
}

func NewRocksDB(name, dir string) (*RocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	//columnFamilyNames := []string{"default", "metadata", "realdata"}

	//bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetBlockCache(gorocksdb.NewLRUCache(1 << 30))
	defaultOpts := gorocksdb.NewDefaultOptions()
	//defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)

	//opts := gorocksdb.NewDefaultOptions()
	//db, columnFamilyHandles, err := gorocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilyNames, []*gorocksdb.Options{opts, opts, opts})
	db, err := gorocksdb.OpenDb(defaultOpts, dbPath)
	//db, err := gorocksdb.OpenDbForReadOnly(defaultOpts, dbPath, false)
	if err != nil {
		fmt.Println("DB open error, try readonly", err)
		db, err = gorocksdb.OpenDbForReadOnly(defaultOpts, dbPath, false)
	}
	if err != nil {
		fmt.Println("DB open error 2", err)
		return nil, err
	}


	ro := gorocksdb.NewDefaultReadOptions()
	//ro.Destroy()
	//ro.SetPinData(true)
	wo := gorocksdb.NewDefaultWriteOptions() // default, false
	wo.SetSync(false)
	database := &RocksDB{
		db:                  db,
		ro:                  ro,
		wo:                  wo,
		columnFamilyHandles: nil, // columnFamilyHandles
	}
	return database, nil
}


//func NewGoLevelDB(name string, dir string) (*GoLevelDB, error) {
//	return NewGoLevelDBWithOpts(name, dir, nil)
//}
//
//func NewGoLevelDBWithOpts(name string, dir string, o *opt.Options) (*GoLevelDB, error) {
//	dbPath := filepath.Join(dir, name+".db")
//	db, err := leveldb.OpenFile(dbPath, o)
//	if err != nil {
//		return nil, err
//	}
//	database := &GoLevelDB{
//		db: db,
//	}
//	return database, nil
//}

// Implements DB.
func (db *RocksDB) get(key []byte) *gorocksdb.Slice {
	key = nonNilBytes(key)
	res, err := db.db.Get(db.ro, key)
	if err != nil {
		//gorocksdb.
		//if err == errors.ErrNotFound {
		//	return nil
		//}
		panic(err)
	}

	return res
}


func (db *RocksDB) Get(key []byte) []byte {
	slice := db.get(key)
	return slice.Data()
}

// Implements DB.
func (db *RocksDB) Has(key []byte) bool {
	return db.get(key).Exists()
}

// Implements DB.
func (db *RocksDB) Set(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) SetSync(key []byte, value []byte) {
	db.Set(key, value)
	// TODO: need to sync options
	//key = nonNilBytes(key)
	//value = nonNilBytes(value)
	//db.
	//err := db.db.Put(key, value, &opt.WriteOptions{Sync: true})
	//if err != nil {
	//	panic(err)
	//}
}

// Implements DB.
func (db *RocksDB) Delete(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(db.wo, key)

	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) DeleteSync(key []byte) {
	db.Delete(key)
	// TODO: need to sync options
	//db.wo.SetSync()
	//key = nonNilBytes(key)
	//err := db.db.Delete(&opt.WriteOptions{Sync: true}, key)
	//if err != nil {
	//	panic(err)
	//}
}

func (db *RocksDB) DB() *gorocksdb.DB {
	db.Close()
	return db.db
}

// Implements DB.
func (db *RocksDB) Close() {
	db.db.Close()
}

// Implements DB.
func (db *RocksDB) Print() {
	fmt.Println(db)
	//db.Stats()
	//str, _ := db.db.GetProperty("leveldb.stats")
	//fmt.Printf("%v\n", str)
	//
	//itr := db.db.NewIterator(nil, nil)
	//for itr.Next() {
	//	key := itr.Key()
	//	value := itr.Value()
	//	fmt.Printf("[%X]:\t[%X]\n", key, value)
	//}
}

// Implements DB.
func (db *RocksDB) Stats() map[string]string {
	keys := []string{
		"rocksdb.num-immutable-mem-table",
		"rocksdb.mem-table-flush-pending",
		"rocksdb.compaction-pending",
		"rocksdb.background-errors",
		"rocksdb.cur-size-active-mem-table",
		"rocksdb.cur-size-all-mem-tables",
		"rocksdb.size-all-mem-tables",
		"rocksdb.num-entries-active-mem-table",
		"rocksdb.num-entries-imm-mem-tables",
		"rocksdb.num-deletes-active-mem-table",
		"rocksdb.num-deletes-imm-mem-tables",
		"rocksdb.estimate-num-keys",
		"rocksdb.estimate-table-readers-mem",
		"rocksdb.is-file-deletions-enabled",
		"rocksdb.num-snapshots",
		"rocksdb.oldest-snapshot-time",
		"rocksdb.num-live-versions",
		"rocksdb.current-super-version-number",
		"rocksdb.estimate-live-data-size",
		"rocksdb.min-log-number-to-keep",
		"rocksdb.min-obsolete-sst-number-to-keep",
		"rocksdb.total-sst-files-size",
		"rocksdb.live-sst-files-size",
		"rocksdb.base-level",
		"rocksdb.estimate-pending-compaction-bytes",
		"rocksdb.num-running-compactions",
		"rocksdb.num-running-flushes",
		"rocksdb.actual-delayed-write-rate",
		"rocksdb.is-write-stopped",
		"rocksdb.estimate-oldest-key-time",
		"rocksdb.block-cache-capacity",
		"rocksdb.block-cache-usage",
		"rocksdb.block-cache-pinned-usage",
	}
	stats := make(map[string]string)
	for _, key := range keys {
		str := db.db.GetProperty(key)
		//if err == nil {
		stats[key] = str
		//}
	}
	return stats
}

//----------------------------------------
// Batch


func (db *RocksDB) NewBatch() Batch {
	batch := gorocksdb.NewWriteBatch()
	return &rocksDBBatch{db, batch}
}

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *rocksDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Write() {
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
func (mBatch *rocksDBBatch) WriteSync() {
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)  // TODO: need to sync options
	//err := mBatch.db.db.Write(mBatch.batch, &opt.WriteOptions{Sync: true})
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
// Close is no-op for goLevelDBBatch.
func (mBatch *rocksDBBatch) Close() {
	mBatch.db.Close()
}

//----------------------------------------
// Iterator
// NOTE This is almost identical to db/c_level_db.Iterator
// Before creating a third version, refactor.

// Implements DB.
func (db *RocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	//itr := db.db.NewIterator(nil, nil)
	return newRocksDBIterator(*itr, start, end, false)
}

// Implements DB.
func (db *RocksDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(*itr, start, end, true)
}

type rocksDBIterator struct {
	source    gorocksdb.Iterator
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
}

var _ Iterator = (*rocksDBIterator)(nil)

func newRocksDBIterator(source gorocksdb.Iterator, start, end []byte, isReverse bool) *rocksDBIterator {
	if isReverse {
		if end == nil {
			source.SeekToLast()
			//source.Last()
		} else {
			source.Seek(end)
			if source.Valid() {
				eoakey := source.Key() // end or after key
				if bytes.Compare(end, eoakey.Data()) <= 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &rocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Implements Iterator.
func (itr *rocksDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator.
func (itr *rocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var start = itr.start
	var end = itr.end
	var key = itr.source.Key().Data()

	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

// Implements Iterator.
func (itr *rocksDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertNoError()
	itr.assertIsValid()
	return cp(itr.source.Key().Data())
}

// Implements Iterator.
func (itr *rocksDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	// See https://github.com/syndtr/goleveldb/blob/52c212e6c196a1404ea59592d3f1c227c9f034b2/leveldb/iterator/iter.go#L88
	itr.assertNoError()
	itr.assertIsValid()
	return cp(itr.source.Value().Data())
}

// Implements Iterator.
func (itr *rocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// Implements Iterator.
func (itr *rocksDBIterator) Close() {
	itr.source.Close()
	//itr.source.Release()
}

func (itr *rocksDBIterator) assertNoError() {
	if err := itr.source.Err(); err != nil {
		panic(err)
	}
}

func (itr rocksDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("rocksDBIterator is invalid")
	}
}
