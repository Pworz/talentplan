package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn      *MvccTxn
	iter     engine_util.DBIterator
	startKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		txn:      txn,
		iter:     iter,
		startKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
		scan.iter = nil
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	for scan.iter.Valid() {
		item := scan.iter.Item()
		key := DecodeUserKey(item.Key())
		ts := decodeTimestamp(item.Key())

		// 只处理小于等于当前事务开始时间戳的记录
		if ts > scan.txn.StartTS {
			scan.iter.Next()
			continue
		}

		// 获取写记录
		value, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}

		// 跳过这个key的后续版本
		nextKey := scan.iter.Item().Key()
		for scan.iter.Valid() && bytes.Equal(DecodeUserKey(nextKey), key) {
			scan.iter.Next()
			if scan.iter.Valid() {
				nextKey = scan.iter.Item().Key()
			}
		}

		// 如果是删除记录，跳过
		if write.Kind == WriteKindDelete {
			continue
		}

		// 获取实际的值
		val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		if err != nil {
			return nil, nil, err
		}

		return key, val, nil
	}

	return nil, nil, nil
}
