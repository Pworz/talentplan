// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	//"github.com/pingcap-incubator/tinykv/log"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Printf("ERROR: storage is nil")
		return nil
	}

	// 获取初始状态
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Printf("Failed to get initial state: %v", err)
		return nil
	}

	// 获取并验证索引范围
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Printf("Failed to get first index: %v", err)
		return nil
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Printf("Failed to get last index: %v", err)
		return nil
	}

	if lastIndex < firstIndex-1 {
		log.Printf("WARNING: lastIndex(%d) < firstIndex(%d)-1", lastIndex, firstIndex)
	}

	// 获取entries
	entries := make([]pb.Entry, 0)
	if firstIndex <= lastIndex {
		entries, err = storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			log.Printf("Failed to get entries: %v", err)
			return nil
		}
	}

	// 创建RaftLog
	raftLog := &RaftLog{
		storage:         storage,
		firstIndex:      firstIndex,
		committed:       hardState.Commit,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
	}

	// 验证初始状态
	raftLog.checkInvariants()

	return raftLog
}

// 检查重要的不变量
func (l *RaftLog) checkInvariants() {
	// 检查 applied <= committed
	if l.applied > l.committed {
		log.Printf("WARNING: applied(%d) > committed(%d)", l.applied, l.committed)
	}

	// 检查 entries 的连续性
	if len(l.entries) > 1 {
		for i := 1; i < len(l.entries); i++ {
			if l.entries[i].GetIndex() != l.entries[i-1].GetIndex()+1 {
				log.Printf("WARNING: discontinuous entries at index %d", i)
			}
		}
	}

	// 检查 firstIndex 正确性
	if len(l.entries) > 0 && l.firstIndex != l.entries[0].GetIndex() {
		log.Printf("WARNING: firstIndex(%d) != first entry index(%d)",
			l.firstIndex, l.entries[0].GetIndex())
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 获取存储中第一个索引
	newFirstIndex, err := l.storage.FirstIndex()
	if err != nil {
		log.Printf("Failed to get storage first index: %v", err)
		return
	}

	// 如果没有需要压缩的条目，直接返回
	if newFirstIndex <= l.firstIndex {
		return
	}

	// 计算需要保留的entries
	if len(l.entries) > 0 {
		// 计算偏移量
		offset := newFirstIndex - l.firstIndex
		if offset >= uint64(len(l.entries)) {
			// 所有entries都需要被压缩
			l.entries = make([]pb.Entry, 0)
		} else {
			// 保留未压缩的entries
			entries := l.entries[offset:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
	}

	// 更新firstIndex
	l.firstIndex = newFirstIndex

	// 验证状态
	if len(l.entries) > 0 && l.firstIndex != l.entries[0].GetIndex() {
		log.Printf("WARNING: firstIndex(%d) != first entry index(%d)",
			l.firstIndex, l.entries[0].GetIndex())
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// 如果没有条目，直接返回nil
	if len(l.entries) == 0 {
		return nil
	}

	// 如果所有条目都已经稳定，返回nil
	if l.stabled >= l.LastIndex() {
		return nil
	}

	// 计算起始索引
	start := max(l.stabled+1-l.firstIndex, 0)
	if start >= uint64(len(l.entries)) {
		return nil
	}

	// 返回未稳定的条目
	result := make([]pb.Entry, len(l.entries[start:]))
	copy(result, l.entries[start:])
	return result
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	if l.committed <= l.applied {
		return nil
	}

	// 计算起始和结束索引
	start := max(l.applied+1-l.firstIndex, 0)
	end := l.committed - l.firstIndex + 1

	if start >= uint64(len(l.entries)) {
		return nil
	}

	// 返回已提交但未应用的条目
	result := make([]pb.Entry, len(l.entries[start:end]))
	copy(result, l.entries[start:end])
	return result
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//返回最后一个索引
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	index, _ := l.storage.LastIndex()
	// 比较快照索引和日志索引，返回较大的一个
	if !IsEmptySnap(l.pendingSnapshot) {
		index = max(index, l.pendingSnapshot.Metadata.Index)
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 检查索引是否在快照范围内
	if len(l.entries) > 0 && i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

// 仿etcd的函数
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("appliedindex(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) commitTo(i uint64) {
	if l.committed < i {
		if l.LastIndex() < i {
			log.Panicf("cimmitedindex(%d) is out of range [lastIndex(%d)]", i, l.LastIndex())
		}
		l.committed = i
	}
}

// 删除传入索引之后的所有条目，并更新 committed、stabled 和 applied 字段
func (l *RaftLog) deleteAfter(index uint64) {
	// 边界检查
	if len(l.entries) == 0 || index < l.FirstIndex() {
		return
	}

	// 计算切片索引
	sliceIndex := index - l.FirstIndex()
	if sliceIndex >= uint64(len(l.entries)) {
		return
	}

	// 删除entries
	l.entries = l.entries[:sliceIndex]

	// 更新索引
	l.stabled = min(index-1, l.stabled)
	l.committed = min(index-1, l.committed)
	l.applied = min(index-1, l.applied)
}
