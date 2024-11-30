package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	// 用于存储和检索键值对
	storage storage.Storage

	// (Used in 4B)
	// 用于管理事务中的锁，确保并发控制
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
// 处理Raft协议的命令，用于集群节点之间的通信
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// Get a reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create an MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// Check if the key is locked
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	// Get the value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 获取所有key的锁
	keys := make([][]byte, 0, len(req.Mutations))
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	if wg := server.Latches.AcquireLatches(keys); wg != nil {
		wg.Wait()
	}
	defer server.Latches.ReleaseLatches(keys)

	// 检查每个key
	for _, m := range req.Mutations {
		// 检查是否已经被锁定
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(m.Key),
			})
			continue
		}

		// 检查写冲突
		write, ts, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && ts >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		// 添加锁
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(m.Op),
		}
		txn.PutLock(m.Key, lock)

		// 根据操作类型处理值
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
		}
	}

	// 写入所有更改
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// 去重 keys
	uniqueKeys := make(map[string][]byte)
	var keys [][]byte
	for _, key := range req.Keys {
		if _, exists := uniqueKeys[string(key)]; !exists {
			uniqueKeys[string(key)] = key
			keys = append(keys, key)
		}
	}

	// 一次性获取所有锁
	if wg := server.Latches.AcquireLatches(keys); wg != nil {
		wg.Wait()
	}
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range keys {
		// 先检查是否已经提交
		write, commitTS, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		// 如果已经有写记录
		if write != nil {
			// 如果是回滚记录，返回错误
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "Already rolled back",
				}
				return resp, nil
			}
			// 如果已经提交过，检查提交版本是否一致
			if commitTS != req.CommitVersion {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
			continue
		}

		// 检查锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// 如果没有锁，说明预写入丢失，跳过这个 key
		if lock == nil {
			continue
		}

		// 如果锁不属于当前事务，返回错误
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}

		// 创建新的写记录
		write = &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}

		// 删除锁并写入记录
		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion, write)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.GetLimit()); {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		if value == nil {
			continue
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		i++
	}
	return &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       pairs,
	}, nil
}
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if write != nil {
		// 事务已经提交或回滚
		if write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
		} else {
			resp.CommitVersion = commitTs
			resp.Action = kvrpcpb.Action_NoAction
		}
		return resp, nil
	}

	// 检查锁
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock == nil {
		// 没有锁，说明事务已经回滚
		write = &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, write)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// 检查锁是否超时
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
		// 锁已超时，清除锁并回滚事务
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		write = &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs, write)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}

	// 锁仍然有效
	resp.Action = kvrpcpb.Action_NoAction
	resp.LockTtl = lock.Ttl
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 去重 keys
	uniqueKeys := make(map[string][]byte)
	var keys [][]byte
	for _, key := range req.GetKeys() {
		if _, exists := uniqueKeys[string(key)]; !exists {
			uniqueKeys[string(key)] = key
			keys = append(keys, key)
		}
	}

	// 一次性获取所有锁
	if wg := server.Latches.AcquireLatches(keys); wg != nil {
		wg.Wait()
	}
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, key := range keys {
		// 检查是否已经提交或回滚
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}
			return &kvrpcpb.BatchRollbackResponse{
				Error: &kvrpcpb.KeyError{
					Abort: "Already committed",
				},
			}, nil
		}

		// 检查并处理锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			// 只有当锁属于当前事务时才删除
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}

		// 无论是否有锁，都写入回滚记录
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	version := req.GetStartVersion()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ResolveLockResponse{RegionError: regionErr.RequestErr}, nil
		}
		return nil, err
	}
	defer reader.Close()

	// 使用 CF Lock 迭代器查找所有锁
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == version {
			keys = append(keys, key)
		}
	}

	resp := &kvrpcpb.ResolveLockResponse{}
	if len(keys) == 0 {
		return resp, nil
	}

	// 根据 CommitVersion 决定提交或回滚
	if req.GetCommitVersion() == 0 {
		// 回滚
		rbReq := &kvrpcpb.BatchRollbackRequest{
			Context:      req.GetContext(),
			StartVersion: req.GetStartVersion(),
			Keys:         keys,
		}
		rbResp, err := server.KvBatchRollback(nil, rbReq)
		if err != nil {
			return nil, err
		}
		resp.RegionError = rbResp.RegionError
		resp.Error = rbResp.Error
	} else {
		// 提交
		commitReq := &kvrpcpb.CommitRequest{
			Context:       req.GetContext(),
			StartVersion:  req.GetStartVersion(),
			Keys:          keys,
			CommitVersion: req.GetCommitVersion(),
		}
		commitResp, err := server.KvCommit(nil, commitReq)
		if err != nil {
			return nil, err
		}
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
