package standalone_storage

import (
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	//添加引用
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := filepath.Join(conf.DBPath, "kv")
	kv := engine_util.CreateDB(kvPath, false)
	raftPath := filepath.Join(conf.DBPath, "raft")
	raft := engine_util.CreateDB(raftPath, true)
	storage := StandAloneStorage{
		engine: engine_util.NewEngines(kv, raft, kvPath, raftPath),
	}
	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	read := false
	txn := s.engine.Kv.NewTransaction(read)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 创建一个写事务
	write := true
	txn := s.engine.Kv.NewTransaction(write)
	defer txn.Discard() // 确保在出错时丢弃事务

	for _, mod := range batch {
		switch m := mod.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(m.Cf, m.Key), m.Value); err != nil {
				return err // 返回错误
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(m.Cf, m.Key)); err != nil {
				return err // 返回错误
			}
		}
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		return err // 返回错误
	}

	return nil // 成功返回 nil
}
