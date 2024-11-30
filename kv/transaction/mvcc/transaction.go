package mvcc

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	// 事务的开始时间戳
	StartTS uint64
	// 存储读取器接口
	Reader storage.StorageReader
	// 存储修改的切片
	writes []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	// 把原来的key加上ts时间戳揉成一个新的key
	encodedKey := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   encodedKey,
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
//
//	返回一个锁，如果键被锁定
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return ParseLock(value)
}

// PutLock adds a key/lock to this transaction.
// 添加一个键/锁到事务中
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
// 查找事务开始时间戳之前的键的值
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// 使用最大时间戳查找
	iter.Seek(EncodeKey(key, math.MaxUint64))

	// 遍历所有版本，找到第一个小于等于startTS的版本
	for iter.Valid() {
		item := iter.Item()
		gotKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)

		// 如果不是同一个key，退出
		if !bytes.Equal(key, userKey) {
			break
		}

		commitTS := decodeTimestamp(gotKey)
		// 找到第一个小于等于startTS的版本
		if commitTS <= txn.StartTS {
			value, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}

			write, err := ParseWrite(value)
			if err != nil {
				return nil, err
			}

			// 如果是删除操作，返回nil
			if write.Kind == WriteKindDelete {
				return nil, nil
			}

			// 使用写记录的StartTS获取实际的值
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		}

		iter.Next()
	}

	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   encodedKey,
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: encodedKey,
			Cf:  engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// 使用最大时间戳查找
	iter.Seek(EncodeKey(key, math.MaxUint64))

	// 遍历所有版本
	for iter.Valid() {
		item := iter.Item()
		gotKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)

		// 如果不是同一个key，返回nil
		if !bytes.Equal(key, userKey) {
			return nil, 0, nil
		}

		// 获取写记录
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		commitTS := decodeTimestamp(gotKey)

		// 如果找到对应的写记录
		if write.StartTS == txn.StartTS {
			return write, commitTS, nil
		}

		// 如果当前版本的commitTS小于startTS，说明没有找到对应的写记录
		if commitTS < txn.StartTS {
			return nil, 0, nil
		}

		// 继续查找下一个版本
		iter.Next()
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// 使用最大时间戳查找
	iter.Seek(EncodeKey(key, math.MaxUint64))

	// 只需要找到第一个匹配的key的写记录即可
	if iter.Valid() {
		item := iter.Item()
		gotKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)

		// 如果不是同一个key，返回nil
		if !bytes.Equal(key, userKey) {
			return nil, 0, nil
		}

		// 获取写记录
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		commitTS := decodeTimestamp(gotKey)
		return write, commitTS, nil
	}

	return nil, 0, nil
}

//下面提供了一系列用于处理键（key）和时间戳（timestamp）编码与解码的函数

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	// Your Code Here (4A).
	encodedKey := codec.EncodeBytes(key)
	// 追加 8 个字节的空间将用于存储编码后的时间戳
	newKey := append(encodedKey, make([]byte, 8)...)
	// 时间戳按位取反后存储
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	// Your Code Here (4A).
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	// Your Code Here (4A).
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	// Your Code Here (4A).
	return ts >> tsoutil.PhysicalShiftBits
}
