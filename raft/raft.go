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
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress
	//Match,Next uint64

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	//加，选举计时器的上限，
	//从electiontimeout 到 (2 * electiontimeout -1)之间的随机值
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// etcd的量，先放着看看后面要不要填坑
	preVote     bool
	checkQuorum bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage)
	//从Storage中读取hardstate和confstate
	hardstate, confstate, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	r := &Raft{
		id:      c.ID,
		Term:    hardstate.Term, //根据hardstate恢复term
		Vote:    hardstate.Vote, //根据hardstate恢复vote
		RaftLog: raftlog,
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower, // 初始都是Follower
		votes:   make(map[uint64]bool),
		// 本来照着etcd的代码写的，但是过不了测试，要删
		// msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		// 坑，放着
		//preVote:          c.preVote,
		//checkQuorum:      c.checkQuorum,
		//leadTransferee:   c.leadTransferee,
		//PendingConfIndex: c.PendingConfIndex,
	}
	//从confstate获取信息初始化prs，（只有Leader的raft.prs有效）
	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, v := range c.peers {
		if v == r.id {
			// 使用committed作为初始Match,这样更安全
			r.Prs[v] = &Progress{
				Match: r.RaftLog.committed,
				Next:  lastIndex + 1,
			}
		} else {
			r.Prs[v] = &Progress{
				Match: 0,
				Next:  1,
			}
		}
	}
	// 仿etcd的函数
	//如果是重启（applied日志不为0），则需要恢复日志状态
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	// 切换为Follower,等待超时进行选举
	// r.becomeFollower(r.Term, None)
	// 上面这里按照etcd的代码，踩了大坑,删了
	r.resetRandomizedElectionTimeout()
	return r
}

// 仿etcd重置选举超时
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// 仿etcd加一个重置函数reset()
func (r *Raft) reset(term uint64) {
	// 当前任期不等于传入的任期，重置
	// 确保一个任期内只会投一次票
	//应该也可以换成 r.Term < term
	if r.Term < term {
		r.Term = term
		r.Vote = None
	}
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.Lead = None
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.heartbeatElapsed = 0
	r.leadTransferee = None
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term) //重置term,vote,选举超时，prs等等
	r.Lead = lead
	if PrintFollowerState != 0 {
		log.Infof("%d become follower, term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	}
	if PrintRaftState != 0 {
		PrintRaftInfo(r)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 切换状态
	// 任期加1
	// 投票给自己
	r.State = StateCandidate
	r.reset(r.Term + 1) //注意leader变成了None
	r.Vote = r.id
	r.votes[r.id] = true

	if PrintCandidateState != 0 {
		log.Infof("%d become candidate, term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	}
	if PrintRaftState != 0 {
		PrintRaftInfo(r)
	}

	granted := 0
	for _, vote := range r.votes {
		if vote {
			granted += 1
		}
	}
	// 多数派同意，成为领导者
	if granted > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// 初始化状态
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = None
	r.votes = make(map[uint64]bool)

	// 初始化 nextIndex 和 matchIndex
	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id].Next = lastIndex + 1
		r.Prs[id].Match = 0
	}

	// 添加一条noop日志
	noop := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     lastIndex + 1,
		Data:      nil,
	}
	r.appendEntry([]*pb.Entry{&noop})

	if PrintLeaderState != 0 {
		log.Infof("%d become leader, term: %d, lastIndex: %d\n", r.id, r.Term, r.RaftLog.LastIndex())
	}
	if PrintRaftState != 0 {
		PrintRaftInfo(r)
	}

	// 立即发送心跳
	r.bcastHeartbeat()

	// 广播 AppendEntries 复制 noop 日志
	r.bcastAppend()
}

// 仿etcd，添加日志
func (r *Raft) appendEntry(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}

	// 创建条目副本
	es := make([]pb.Entry, 0, len(entries))
	lastIndex := r.RaftLog.LastIndex()

	for i, e := range entries {
		// 处理配置变更
		if e.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex < e.Index {
				r.PendingConfIndex = e.Index
			}
		}

		// 创建新条目
		entry := pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Data:      e.Data,
		}

		// 设置索引
		if e.Index == 0 {
			entry.Index = lastIndex + uint64(i) + 1
		} else {
			entry.Index = e.Index
		}

		es = append(es, entry)
	}

	// 直接追加到日志条目
	r.RaftLog.entries = append(r.RaftLog.entries, es...)

	// 只有 Leader 更新 Progress
	if r.State == StateLeader {
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
	}
}

func (r *Raft) maybeCommit() bool {
	if r.State != StateLeader {
		return false
	}

	// 收集所有节点的匹配索引
	matches := make([]uint64, 0, len(r.Prs))
	for _, prs := range r.Prs {
		matches = append(matches, prs.Match)
	}

	// 排序并找到中位数
	sort.Sort(sort.Reverse(uint64Slice(matches)))
	mid := matches[len(matches)/2]

	// 如果中位数大于当前提交索引，且该位置的任期等于当前任期
	if mid > r.RaftLog.committed {
		term, err := r.RaftLog.Term(mid)
		if err == nil && term == r.Term {
			r.RaftLog.commitTo(mid)
			return true
		}
	}
	return false
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return true
		}
		return false
	}

	entries := make([]*pb.Entry, 0)
	if r.Prs[to].Next <= r.RaftLog.LastIndex() {
		// 确保 Next 不小于 firstIndex
		startIndex := r.Prs[to].Next
		if startIndex < r.RaftLog.firstIndex {
			startIndex = r.RaftLog.firstIndex
		}

		// 计算切片索引
		sliceIndex := startIndex - r.RaftLog.firstIndex

		// 添加所有需要同步的日志条目
		for i := sliceIndex; i < uint64(len(r.RaftLog.entries)); i++ {
			entry := r.RaftLog.entries[i]
			entries = append(entries, &pb.Entry{
				EntryType: entry.EntryType,
				Term:      entry.Term,
				Index:     entry.Index,
				Data:      entry.Data,
			})
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})

	return true
}

// sendSnapshot sends a snapshot to the given peer.
func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// 假如快照暂时不可用
		if err == ErrSnapshotTemporarilyUnavailable {
			return false
		}
		panic(err)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return true
}

// 与sendAppend()对称, 发送响应
func (r *Raft) sendAppendResponse(to uint64, reject bool, term uint64, index uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex()})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat})
}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject})
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject})
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	})
}

// tick advances the internal logical clock by a single tick.
// 计时器，逻辑时钟
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateCandidate || r.State == StateFollower {
		if PrintRaftState != 0 {
			fmt.Println("tickelection@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		}
		r.tickElection()
	}
	if r.State == StateLeader {
		if PrintRaftState != 0 {
			fmt.Println("tickheartbeat@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		}
		r.tickHeartbeat()
	}
}

// 选举计时
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.campaign()
	}
}

// 心跳计时
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		if r.State == StateLeader {
			// 处理leader transfer
			if r.leadTransferee != None {
				// 等待transfer完成
				return
			}
			r.bcastHeartbeat()
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 模拟etcd，简单版本的prevote机制
	_, ok := r.Prs[r.id]
	if !ok && m.GetMsgType() == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.RaftLog.pendingSnapshot != nil {
			return
		}
		if PrintMessage != 0 {
			log.Infof("stepFollower message: %+v\n", m)
		}
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		if PrintMessage != 0 {
			log.Infof("stepFollower message: %+v\n", m)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		if PrintMessage != 0 {
			log.Infof("stepFollower message: %+v\n", m)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		if PrintMessage != 0 {
			log.Infof("stepFollower message: %+v\n", m)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		if PrintMessage != 0 {
			log.Infof("stepFollower message: %+v\n", m)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTransferLeader,
			To:      r.Lead,
			From:    m.From,
		})
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if PrintMessage != 0 {
			log.Infof("stepCandidate message: %+v\n", m)
		}
		r.campaign()
	case pb.MessageType_MsgAppend:
		if PrintMessage != 0 {
			log.Infof("stepCandidate message: %+v\n", m)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		if PrintMessage != 0 {
			log.Infof("stepCandidate message: %+v\n", m)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if PrintMessage != 0 {
			log.Infof("stepCandidate message: %+v\n", m)
		}
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTransferLeader,
			To:      r.Lead,
			From:    m.From,
		})
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handlePropose(m)
	case pb.MessageType_MsgRequestVote:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handleAppendResponse(m)
	case pb.MessageType_MsgSnapshot:
		if PrintMessage != 0 {
			log.Infof("stepLeader message: %+v\n", m)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m)
	}
}

// campaign start election
// 仿etcd的函数,发起选举
func (r *Raft) campaign() {
	// Your Code Here (2A).
	// 切换状态
	r.becomeCandidate()
	r.bcastVoteRequst()
}

// 广播日志请求
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// 广播心跳
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// 广播投票
func (r *Raft) bcastVoteRequst() {
	//fmt.Printf("%d vote req\n", r.id)
	lastIndex := r.RaftLog.LastIndex()
	lastTerm := mustTerm(r.RaftLog.Term(lastIndex))
	for peer := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex})
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 任期小于自己的任期，拒绝请求
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
		return
	}
	// 任期大于等于自己的任期，更新自己的任期
	r.becomeFollower(m.Term, m.From)

	// 检查prevLogIndex是否超出范围
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
		return
	}

	// 检查日志是否匹配
	if m.Index >= r.RaftLog.firstIndex {
		prevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || prevLogTerm != m.LogTerm {
			// 返回我们的LastIndex，帮助leader更快找到同步点
			r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
			return
		}
	}

	// 处理日志条目
	if len(m.Entries) > 0 {
		// 找到第一个冲突的日志条目
		firstConflict := -1
		for i, entry := range m.Entries {
			if entry.Index < r.RaftLog.firstIndex {
				continue
			}
			if entry.Index > r.RaftLog.LastIndex() {
				break
			}
			term, err := r.RaftLog.Term(entry.Index)
			if err != nil || term != entry.Term {
				firstConflict = i
				break
			}
		}

		// 如果有冲突，删除从冲突点开始的所有日志
		if firstConflict != -1 {
			r.RaftLog.deleteAfter(m.Entries[firstConflict].Index - 1)
			r.appendEntry(m.Entries[firstConflict:])
		} else if len(m.Entries) > 0 {
			lastNewEntry := m.Entries[len(m.Entries)-1].Index
			if lastNewEntry > r.RaftLog.LastIndex() {
				// 只追加新的日志条目
				startIdx := 0
				for i, entry := range m.Entries {
					if entry.Index > r.RaftLog.LastIndex() {
						startIdx = i
						break
					}
				}
				r.appendEntry(m.Entries[startIdx:])
			}
		}
	}

	// 更新committed
	// 如果是空日志(心跳),committed应为index和commit中的最小值
	if len(m.Entries) == 0 {
		r.RaftLog.commitTo(min(m.Index, m.Commit))
	} else if m.Commit > r.RaftLog.committed {
		// 如果有新日志,committed应为LastIndex和commit中的最小值
		r.RaftLog.commitTo(min(m.Commit, r.RaftLog.LastIndex()))
	}

	// 发送响应
	r.sendAppendResponse(m.From, false, r.Term, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// 如果发现更大的任期号,转为follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	// 只处理当前任期的日志响应
	if m.Term != r.Term {
		return
	}

	// 检查重复响应 - 只处理最新的响应
	if m.Index < r.Prs[m.From].Next-1 {
		return
	}

	if !m.Reject {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

		// 如果有新的日志被提交,发送空AppendEntries来更新follower的committed
		oldCommitted := r.RaftLog.committed
		if r.maybeCommit() && r.RaftLog.committed > oldCommitted {
			r.bcastAppend()
		}
	} else {
		// 处理拒绝响应
		// 如果index太旧,发送快照
		if m.Index < r.RaftLog.FirstIndex() {
			r.sendSnapshot(m.From)
			return
		}
		// 只有当index等于Next-1时才回退Next并重试
		if m.Index == r.Prs[m.From].Next-1 {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
		}
		return
	}

	// 处理leader转移
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// 检查 term
	if m.Term < r.Term {
		// 返回更高 term 的响应
		r.sendHeartbeatResponse(m.From, true)
		return
	}

	// term 相同或更大，成为 follower
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// 转换 leader
	if m.From != r.Lead {
		r.Lead = m.From
	}
	// 重置时间
	r.electionElapsed = 0
	// 发送响应
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// 当前节点是旧领导者，向 m.From 发送心跳
	// 但 m.From 是新领导者
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From) // 切换状态
		return
	}
	// 因为我们没有追加超时机制
	// 所以在这里进行追加操作
	r.bcastAppend() // 广播追加日志请求
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != None {
		return
	}

	// 获取最后一个日志索引
	lastIndex := r.RaftLog.LastIndex()

	// 为新条目设置正确的任期和索引
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
	}

	// 追加日志条目
	r.appendEntry(m.Entries)

	// 更新自身Progress并尝试提交
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 单节点集群时可以直接提交
	if len(r.Prs) == 1 {
		r.RaftLog.commitTo(r.Prs[r.id].Match)
	} else {
		// 多节点集群时检查是否可以提交
		oldCommitted := r.RaftLog.committed
		if r.maybeCommit() && r.RaftLog.committed > oldCommitted {
			// 如果有新的提交,广播空AppendEntries来更新follower
			r.bcastAppend()
		} else {
			// 否则只需要复制新日志
			r.bcastAppend()
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//fmt.Printf("%d handle vote %+v\n", r.id, m)
	// 任期小于自己的任期，拒绝投票
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// 添加 PreVote 处理
	//  if r.preVote {
	//     // 检查是否可以成为候选人
	//     canVote := r.canVoteFor(m.From, m.LogTerm, m.Index)
	//     if !canVote {
	//         r.sendRequestVoteResponse(m.From, true)
	//         return
	//     }
	// }
	// 占个坑后续再来实现，如果有合法leader，拒绝
	// 更改任期和状态
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	}
	// 前面拒绝了请求任期小的情况，下面都是请求任期合法的情况
	// 先默认拒绝投票
	Reject := true
	// 同时满足两个if判断，也就是日志最新，可以投票
	// 检查是否符合两种种条件中的一种
	// 要么没投，要么投过对方,要么任期比对方小
	if r.Vote == None || r.Vote == m.From || r.Term < m.Term {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		// 判断对方的日志数据是否最新
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			// 同时满足两个条件，不拒绝投票
			Reject = false
		}
	}
	// 发送投票响应
	r.sendRequestVoteResponse(m.From, Reject)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//fmt.Printf("%d handle voteResp: %+v\n", r.id, m)
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = m.From
		return
	}
	// 不拒绝，则记录赞成票
	if !m.Reject {
		//log.Infof("%d receive agree from %d\n", r.id, m.From)
		r.votes[m.From] = true
	}
	granted := 0
	dis := 0
	for _, vote := range r.votes {
		if vote {
			granted += 1
		} else {
			dis += 1
		}
	}
	// 多数派同意，成为领导者
	if granted > len(r.Prs)/2 {
		r.becomeLeader()
	} else if 2*dis > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
		return
	}
	r.becomeFollower(max(r.Term, meta.Term), m.From)
	// 清空日志
	if len(r.RaftLog.entries) > 0 {
		if meta.Index >= r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else {
			r.RaftLog.entries = r.RaftLog.entries[meta.Index-r.RaftLog.FirstIndex()+1:]
		}
	}
	// 修改信息
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.firstIndex = meta.Index + 1
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.Prs = make(map[uint64]*Progress)
	// 使用 ConfState对 Prs 进行初始化
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	// m.From is transfer target
	if m.From == r.id {
		return
	}
	if r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1, Match: 0}
		r.PendingConfIndex = r.RaftLog.LastIndex()
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		r.PendingConfIndex = r.RaftLog.LastIndex()
		if r.State == StateLeader && len(r.Prs) > 0 {
			r.maybeCommit()
		}
	}
}
func PrintRaftInfo(r *Raft) {
	fmt.Printf("Raft Node Information:\n")
	fmt.Printf("Node ID: %d\n", r.id)
	fmt.Printf("Current Term: %d\n", r.Term)
	fmt.Printf("Vote: %d\n", r.Vote)

	if r.RaftLog != nil {
		fmt.Printf("Raft Log:\n")
	}

	fmt.Printf("Log Replication Progress of Each Peers:\n")
	for peerID, progress := range r.Prs {
		fmt.Printf("  Peer ID: %d, Match: %d, Next: %d\n", peerID, progress.Match, progress.Next)
	}

	fmt.Printf("Node State: %s\n", r.State)

	fmt.Printf("Votes Records:\n")
	for voterID, vote := range r.votes {
		fmt.Printf("  Voter ID: %d, Vote: %v\n", voterID, vote)
	}

	fmt.Printf("Messages to Send:\n")
	for _, msg := range r.msgs {
		fmt.Printf("  %+v\n", msg)
	}

	fmt.Printf("Leader ID: %d\n", r.Lead)
	fmt.Printf("Heartbeat Interval (heartbeatTimeout): %d\n", r.heartbeatTimeout)
	fmt.Printf("Election Interval Baseline (electionTimeout): %d\n", r.electionTimeout)
	fmt.Printf("Randomized Election Interval (randomizedElectionTimeout): %d\n", r.randomizedElectionTimeout)
	fmt.Printf("Ticks since Last Heartbeat Interval (heartbeatElapsed): %d\n", r.heartbeatElapsed)
	fmt.Printf("Ticks since Last Election Interval (electionElapsed): %d\n", r.electionElapsed)
	fmt.Printf("Leader Transfer Target ID (leadTransferee): %d\n\n\n", r.leadTransferee)
}

const PrintRaftState = 0
const PrintMessage = 0
const PrintLeaderState = 0
const PrintFollowerState = 0
const PrintCandidateState = 0
