package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in role 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	VoteForInvalid   = -1
	BaseElectTimeOut = 150
	HeartBeatTimeOut = 500
	OneHeartBeatLag  = 30
	ApplyLogLag      = 100
	ApplyWaitTimeOut = 5 * time.Millisecond
	RPCTimeOut       = 80
)

const (
	EmptyCmdIdx  = 0
	EmptyCmdTerm = 0
)

type Role int

const (
	FOLLOWER  Role = 0
	LEADER    Role = 1
	CANDIDATE Role = 2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	ReceiveTerm  int // 在哪一任上收到消息

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // receive client msg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	readLockCount  int32
	writeLockCount int32

	id                 int  // 当前节点的id
	leader             int  // 当前集群的leader
	role               Role // 当前节点的角色
	electionTimer      *time.Timer
	heartBeatTimer     []*time.Timer
	waitHeartBeatTimer *time.Timer
	commitUpdateTimer  *time.Timer
	applyLogTimer      *time.Timer

	// exists stably in all server as follows（持久数据）
	currentTerm   int // last Term of server knowing
	voteFor       int // 当前任期内收到选票的Candidate id（没有就为-1）
	snapshotMutex sync.RWMutex
	snapshot      []*ApplyMsg // 1号用于更新，0号用于使用
	log           []ApplyMsg  // 每个条目包含状态机的要执行命令和从 Leader 处收到时的任期号, 0这个位置可能要丢弃

	// exists unstably in all server as follows(非持久数据)
	commitIndex int // 已知的被提交的最大日志条目的索引值（从0开始递增）
	lastApplied int // 被状态机执行的最大日志条目的索引值（从0开始递增）

	// exists unstably in leader server as follows(非持久数据)
	nextIndex  []int // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为 Leader 上一条日志的索引值+1）
	matchIndex []int // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
}

func (rf *Raft) RLock() {
	rf.snapshotMutex.RLock()
	/*val := */ atomic.AddInt32(&rf.readLockCount, 1)
	//DPrintf("server=%d get read lock. read lock=%d, write lock=%d\n", rf.id, val, atomic.LoadInt32(&rf.writeLockCount))
}

func (rf *Raft) RUnlock() {
	rf.snapshotMutex.RUnlock()
	/*val :=*/ atomic.AddInt32(&rf.readLockCount, -1)
	//DPrintf("server=%d give up read lock. read lock=%d, write lock=%d\n", rf.id, val, atomic.LoadInt32(&rf.writeLockCount))
}

func (rf *Raft) Lock() {
	rf.snapshotMutex.Lock()
	/*val := */ atomic.AddInt32(&rf.writeLockCount, 1)
	//DPrintf("server=%d get write lock. read lock=%d, write lock=%d\n", rf.id, atomic.LoadInt32(&rf.readLockCount), val)
}

func (rf *Raft) Unlock() {
	rf.snapshotMutex.Unlock()
	/*val :=*/ atomic.AddInt32(&rf.writeLockCount, -1)
	//DPrintf("server=%d give up write lock. read lock=%d, write lock=%d\n", rf.id, atomic.LoadInt32(&rf.readLockCount), val)
}

// return currentTerm and whether this server believes it is the leader.
// 返回当前任期和服务器是否认为自己是leader
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	// Your code here (2A).
	return term, isLeader
}

func (rf *Raft) changeRole(role Role) {
	switch role {
	case CANDIDATE:
		{
			rf.role = CANDIDATE
			rf.currentTerm++
			rf.voteFor = rf.id
			rf.leader = VoteForInvalid
			DPrintf("id=%d vote to itself, term=%d, time=%d\n", rf.id, rf.currentTerm, time.Now().UnixMilli())
			if rf.waitHeartBeatTimer != nil {
				rf.waitHeartBeatTimer.Stop()
			}
			rf.resetElectTimeOut()
		}
	case LEADER:
		{
			rf.role = LEADER
			rf.electionTimer.Stop()

			rf.nextIndex = make([]int, len(rf.peers))
			lastIndex := rf.lastLogIndex()
			for i := range rf.nextIndex {
				rf.nextIndex[i] = lastIndex + 1
			}
			rf.matchIndex = make([]int, len(rf.peers))
			DPrintf("all matchIndex reset\n")
			for i := range rf.matchIndex {
				rf.matchIndex[i] = lastIndex
			}
			rf.fastNotifyLeaderChangeAndRegisterHeartBeat()
		}
	case FOLLOWER:
		{
			if rf.role == LEADER {
				DPrintf("id=%d change to follower from leader\n", rf.id)
			}
			rf.role = FOLLOWER
			rf.resetWaitHeartBeatTimer()
		}
	default:
		panic("unknown role")
	}
}

func randomElectTimeOut() time.Duration {
	return time.Duration(BaseElectTimeOut+rand.Int63()%BaseElectTimeOut) * time.Millisecond
}

func snapshotInit(snapshot *ApplyMsg, valid bool, term int, index int, data []byte) {
	snapshot.SnapshotValid = valid
	snapshot.SnapshotTerm = term
	snapshot.SnapshotIndex = index
	snapshot.Snapshot = data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	labgob.Register(ApplyMsg{})
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.log)
	raftState := buf.Bytes()

	var snapState []byte
	if rf.snapshot[0].SnapshotValid {
		buf = new(bytes.Buffer)
		e = labgob.NewEncoder(buf)
		e.Encode(rf.snapshot[0].SnapshotIndex)
		snapshot := []interface{}{*rf.snapshot[0]}
		e.Encode(snapshot)
		snapState = buf.Bytes()
	}
	rf.persister.Save(raftState, snapState)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftState []byte, snapState []byte) {
	if raftState == nil || len(raftState) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var commitIndex int
	var log []ApplyMsg
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&log) != nil {
		//error...
		DPrintf("server=%d read data error\n", rf.id)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.log = log
	}
	if snapState == nil || len(snapState) < 1 {
		return
	}
	r = bytes.NewBuffer(snapState)
	d = labgob.NewDecoder(r)
	DPrintf("snapshot len=%d\n", len(snapState))
	var lastLogIndex int
	var data []interface{}
	if d.Decode(&lastLogIndex) != nil ||
		d.Decode(&data) != nil {
		DPrintf("server=%d read snapshot data error\n", rf.id)
	} else {
		snapshot := data[0].(ApplyMsg)
		snapshotInit(rf.snapshot[0], snapshot.SnapshotValid, snapshot.SnapshotTerm, snapshot.SnapshotIndex, snapshot.Snapshot)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 是可以让Follower进行创建快照的，因为快照中的内容必须是已经提交的
	if rf.snapshot[0].SnapshotValid && index < rf.snapshot[0].SnapshotIndex || rf.commitIndex < index {
		return
	}
	DPrintf("server=%d调用Snapshot, index=%d\n", rf.id, index)
	rf.mu.Lock()
	rf.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.Unlock()
	}()
	if rf.snapshot[0].SnapshotValid && index < rf.snapshot[0].SnapshotIndex || rf.commitIndex < index {
		return
	}
	snapshotLastIndexInLog := rf.GenLogIdx(index)
	msg := rf.log[snapshotLastIndexInLog]
	rf.snapshot[0].SnapshotValid = true
	rf.snapshot[0].SnapshotIndex = index
	rf.snapshot[0].SnapshotTerm = msg.ReceiveTerm
	rf.snapshot[0].Snapshot = snapshot
	rf.log = rf.log[snapshotLastIndexInLog+1:]
	//rf.mu.Lock()
	rf.persist()
	//rf.mu.Unlock()
	DPrintf("server=%d snapshot build success. logLen=%d\n", rf.id, len(rf.log))
}

type InstallSnapshotArgs struct {
	Term              int    // Leader的任期
	LeaderId          int    // 为了Follower能重定向到客户端
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	LastIncludeTerm   int    // 快照中包含的最后日志条目的任期号
	Offset            int    // 分块在快照中的偏移量, 便于快照分多次进行传输
	Data              []byte // 快照块的原始数据
	Done              bool   // 如果是最后一块数据则为 true, 与Offset搭配使用
}

type InstallSnapshotReply struct {
	Term int // currentTerm，用于 Leader 更新自己的任期
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetWaitHeartBeatTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return
	}

	rf.RLock()
	// 由于snapshot都是已经提交的数据，故snapshot可以不比较term
	if rf.snapshot[0].SnapshotValid &&
		rf.snapshot[0].SnapshotIndex >= args.LastIncludedIndex {
		rf.RUnlock()
		return
	}
	rf.RUnlock()
	DPrintf("follower=%d install snapshot, term=%d, index=%d, offset=%d, done=%v\n"+
		"follower=%d current snapshot len=%d\n", rf.id, args.LastIncludeTerm, args.LastIncludedIndex, args.Offset,
		args.Done, rf.id, len(rf.snapshot[1].Snapshot))
	if args.Offset == 0 {
		rf.snapshot[1].Snapshot = args.Data
	} else {
		curIndex := len(rf.snapshot[1].Snapshot)
		// 处理因为延迟而没有收到的数据块
		if args.Offset >= curIndex {
			for args.Offset > curIndex {
				rf.snapshot[1].Snapshot = append(rf.snapshot[1].Snapshot, 0)
				curIndex++
			}
			rf.snapshot[1].Snapshot = append(rf.snapshot[1].Snapshot, args.Data...)
		} else {
			// 处理延迟收到的数据块
			for offset, i := args.Offset, 0; i < len(args.Data); i++ {
				rf.snapshot[1].Snapshot[offset] = args.Data[i]
				offset++
			}
		}
	}
	if !args.Done {
		return
	}

	rf.Lock()
	defer func() {
		rf.persist()
		rf.Unlock()
	}()
	lastRemoveIdx := rf.GenLogIdx(args.LastIncludedIndex)
	// 完成接收到的快照的最终初始化
	rf.snapshot[1].SnapshotTerm = args.LastIncludeTerm
	rf.snapshot[1].SnapshotIndex = args.LastIncludedIndex
	rf.snapshot[1].SnapshotValid = true
	// 把原快照丢弃
	snapshotInit(rf.snapshot[0], false, 0, 0, make([]byte, 0))
	// 把新快照投入使用
	tmp := rf.snapshot[0]
	rf.snapshot[0] = rf.snapshot[1]
	rf.snapshot[1] = tmp
	// TODO 进行日志丢弃
	if len(rf.log) <= lastRemoveIdx+1 ||
		(rf.log[lastRemoveIdx].CommandIndex == args.LastIncludedIndex &&
			rf.log[lastRemoveIdx].ReceiveTerm == args.LastIncludeTerm) {
		rf.log = rf.log[:0]
	} else {
		rf.log = rf.log[lastRemoveIdx+1:]
	}
	DPrintf("follower=%d install snapshot finished\n", rf.id)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 这些是AppendLog的参数, RequestVoteArgs不需要那么多
	Term         int // 任期号
	CandidateId  int // Leader 的 id，为了其他服务器能重定向到客户端
	PrevLogIndex int // 最新日志的之前的索引值
	PrevLogTerm  int // 最新日志之前的日志的 Leader 任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	CurrTerm    int  // 当前任期
	VoteGranted bool // 是否赢得了选票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrTerm = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		//DPrintf("server=%d, currentTerm=%d >= args.Term=%d, reject to vote to server=%d\n", rf.id, rf.currentTerm, args.Term, args.CandidateId)
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == LEADER {
			return
		}
		if rf.voteFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			return
		}
	}
	// 当前任期未投票，则需要投票
	defer rf.persist()
	//originTerm := rf.currentTerm
	if args.Term > rf.currentTerm {
		// 可能是leader断网之后，发现其他节点正在选举
		if rf.role == LEADER {
			//DPrintf("server=%d rf.currentTerm=%d, args.Term=%d\n", rf.id, rf.currentTerm, args.Term)
			rf.changeRole(FOLLOWER)
		}
		rf.currentTerm = args.Term
		// 加入投票
		rf.leader = VoteForInvalid
		rf.voteFor = VoteForInvalid
	}
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if lastLogTerm > args.PrevLogTerm || (args.PrevLogTerm == lastLogTerm && args.PrevLogIndex < lastLogIndex) {
		DPrintf("server=%d currentTerm=%d > args.Term=%d, vote reject\n", rf.id, rf.currentTerm, args.Term)
		return
	}

	rf.currentTerm = args.Term
	rf.voteFor = args.CandidateId
	reply.CurrTerm = rf.currentTerm
	reply.VoteGranted = true
	rf.changeRole(FOLLOWER)
	rf.electionTimer.Stop()
	DPrintf("server=%d currentTerm=%d, lastLogIndex=%d, lastLogTerm=%d, args.PrevLogTerm=%d, args.PrevLogIndex=%d, vote server=%d\n",
		rf.id, rf.currentTerm, lastLogIndex, lastLogTerm, args.PrevLogTerm, args.PrevLogIndex, args.CandidateId)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	end := make(chan bool, 1)
	stop := time.NewTimer(RPCTimeOut * time.Millisecond)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		end <- ok
	}()
	select {
	case <-stop.C:
		return false
	case ok := <-end:
		return ok
	}
}

func (rf *Raft) lastLogIndex() int {
	if !rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		return EmptyCmdIdx
	} else if rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		return rf.snapshot[0].SnapshotIndex
	} else {
		return rf.log[len(rf.log)-1].CommandIndex
	}
}

func (rf *Raft) lastLogTerm() int {
	if !rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		return EmptyCmdTerm
	} else if rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		return rf.snapshot[0].SnapshotTerm
	} else {
		return rf.log[len(rf.log)-1].ReceiveTerm
	}
}

func (rf *Raft) lastLogTermIndex() (prevLogTerm int, prevLogIndex int) {
	// 集群刚启动时就是没有日志的
	if !rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		prevLogIndex = EmptyCmdIdx
		prevLogTerm = EmptyCmdTerm
	} else if rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		prevLogIndex = rf.snapshot[0].SnapshotIndex
		prevLogTerm = rf.snapshot[0].SnapshotTerm
	} else {
		prevLogIndex = rf.log[len(rf.log)-1].CommandIndex
		// FIXME: rf.GenLogIdx(prevLogIndex)仍有返回-1的可能性
		prevLogTerm = rf.log[rf.GenLogIdx(prevLogIndex)].ReceiveTerm
	}
	return
}

func (rf *Raft) startElect(isHeartBeatTimeOut bool) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == LEADER {
		return true
	}
	// 每一任都必须为两个结果之一: 选举失败，选举成功
	if !isHeartBeatTimeOut && rf.leader != VoteForInvalid {
		return true
	}
	rf.changeRole(CANDIDATE)
	prevLogTerm, prevLogIndex := rf.lastLogTermIndex()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	rf.persist()
	voteCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch *chan bool, index int) {
			var reply RequestVoteReply
			//DPrintf("id=%d send to id=%d\n", rf.id, index)
			end := false
			for !end && rf.role == CANDIDATE {
				end = rf.sendRequestVote(index, &args, &reply)
			}
			if !end {
				return
			}
			votesCh <- reply.VoteGranted
			if reply.CurrTerm > args.Term {
				rf.currentTerm = reply.CurrTerm
				rf.changeRole(FOLLOWER)
				rf.persist()
			}
		}(&votesCh, index)
	}
	end := false
	for !end {
		select {
		case <-rf.electionTimer.C:
			{
				rf.changeRole(FOLLOWER)
				rf.resetElectTimeOut()
				return false
			}
		case r := <-votesCh:
			{
				chResCount++
				if r {
					voteCount++
				}

				maxVoteCount := len(rf.peers)
				if chResCount == maxVoteCount || voteCount > maxVoteCount/2 || chResCount-voteCount > maxVoteCount/2 {
					end = true
				}
			}
		}
	}

	if voteCount <= len(rf.peers)/2 {
		rf.changeRole(FOLLOWER)
		//DPrintf("id=%d get vote less than 1/2 votes. grantedCount=%d\n", rf.id, voteCount)
		return false
	}

	// 保证当前任期与选举后任期一致, 保证在期间没有收到过新的leader的心跳
	if rf.currentTerm == args.Term && rf.leader == VoteForInvalid && rf.role == CANDIDATE {
		DPrintf("id=%d become leader, lastLogIdx=%d, leaderCommit=%d\n", rf.id, rf.lastLogIndex(), rf.commitIndex)
		rf.changeRole(LEADER)
		rf.persist()
		//DPrintf("id=%d become leader, lastLogIdx=%d, leaderCommit=%d\n", rf.id, rf.lastLogIndex(), rf.commitIndex)
	} else {
		rf.changeRole(FOLLOWER)
		rf.electionTimer.Stop()
	}
	return true
}

type AppendEntriesArgs struct {
	Term         int        // 任期号
	LeaderId     int        // Leader 的 id，为了其他服务器能重定向到客户端
	PrevLogIndex int        // 最新日志的之前的索引值
	PrevLogTerm  int        // 最新日志之前的日志的 Leader 任期号
	Entries      []ApplyMsg // 将要存储的日志条目，为了效率有时会超过一条。表示heartbeat时为空
	LeaderCommit int        // leader提交的日志条目索引值
}

type AppendEntriesReply struct {
	Term      int  // 当前任期号
	Success   bool // append操作被接受了
	NextIndex int
}

func min(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) genAppendEntriesArgs(server int, maxLogCount int) AppendEntriesArgs {
	matchIndex := rf.matchIndex[server]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.id,
		LeaderCommit: rf.commitIndex,
	}
	if !rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		args.Entries = nil
		args.PrevLogTerm, args.PrevLogIndex = EmptyCmdTerm, EmptyCmdIdx
	} else if rf.snapshot[0].SnapshotValid && len(rf.log) == 0 {
		args.Entries = nil
		args.PrevLogTerm, args.PrevLogIndex = rf.snapshot[0].SnapshotTerm, rf.snapshot[0].SnapshotIndex
	} else if rf.lastLogIndex() >= rf.nextIndex[server] {
		if matchIndex == EmptyCmdIdx {
			//DPrintf("follower=%d's match index=0 in leader=%d's list\n", server, rf.id)
			args.PrevLogTerm, args.PrevLogIndex = EmptyCmdTerm, EmptyCmdIdx
		} else {
			if rf.snapshot[0].SnapshotValid && rf.snapshot[0].SnapshotIndex == matchIndex {
				args.PrevLogIndex = rf.snapshot[0].SnapshotIndex
				args.PrevLogTerm = rf.snapshot[0].SnapshotTerm
			} else if rf.snapshot[0].SnapshotValid && rf.snapshot[0].SnapshotIndex > matchIndex {
				panic("matchIndex=" + strconv.Itoa(matchIndex) + "< snapshotIdx=" + strconv.Itoa(rf.snapshot[0].SnapshotIndex) + ", cannot build append args\n")
			} else {
				prevCommand := rf.log[rf.GenLogIdx(matchIndex)]
				args.PrevLogIndex = prevCommand.CommandIndex
				args.PrevLogTerm = prevCommand.ReceiveTerm
			}
		}
		//DPrintf("genArgs nextIndex=%d, sender=%d, receiver=%d, maxLogCount=%d\n", rf.nextIndex[server], rf.id, server, maxLogCount)
		if maxLogCount > 0 {
			idx := rf.GenLogIdx(rf.nextIndex[server])
			if rf.nextIndex[server]+maxLogCount <= len(rf.log) {
				args.Entries = rf.log[idx : idx+maxLogCount]
			} else {
				args.Entries = rf.log[idx:]
			}
		}
	} else {
		// 用于同步matchIndex和nextIndex
		args.Entries = nil
		args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()
	}
	//DPrintf("genArgs result=%v. It's leader=%d sending to follower=%d\n", args, rf.id, server)
	return args
}

func (rf *Raft) SendAppend(server int, maxAppendCount int) {
	stop := time.NewTimer(RPCTimeOut * time.Millisecond)
	for {
		if rf.role != LEADER || (rf.snapshot[0].SnapshotValid &&
			rf.nextIndex[server] <= rf.snapshot[0].SnapshotIndex) {
			break
		}
		args := rf.genAppendEntriesArgs(server, maxAppendCount)
		rf.RUnlock()
		reply := AppendEntriesReply{}
		successChan := make(chan bool, 1)
		success := false
		stop.Stop()
		stop.Reset(RPCTimeOut * time.Millisecond)
		go func() {
			if maxAppendCount == 0 {
				DPrintf("leader=%d发送心跳to follower=%d\n", rf.id, server)
			}
			res := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			successChan <- res
		}()
		select {
		case <-stop.C:
		case success = <-successChan:
		}
		if !success {
			if maxAppendCount == 0 {
				DPrintf("leader=%d send appendEntries RPC to follower=%d fail\n", rf.id, server)
			}
			rf.RLock()
			break
		} else if reply.Success {
			// 成功并且有发送日志，则需要更新matchIndex和nextIndex
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].CommandIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				//DPrintf("leader=%d update follower=%d's match index to %d\n", rf.id, server, rf.matchIndex[server])
				if args.Entries[len(args.Entries)-1].ReceiveTerm == rf.currentTerm {
					rf.mu.Lock()
					rf.RLock()
					rf.updateCommit()
					rf.persist()
					rf.RUnlock()
					rf.mu.Unlock()
				}
			}
			rf.RLock()
			break
			// 收到的回应任期比当前任期大，说明当前节点不是leader
		} else if reply.Term > rf.currentTerm {
			rf.changeRole(FOLLOWER)
			rf.RLock()
			break
		} else {
			rf.nextIndex[server] = reply.NextIndex
			rf.matchIndex[server] = reply.NextIndex - 1
			//DPrintf("no find follower=%d's match log, nextIndex=%d, matchIndex=%d\n", server, rf.nextIndex[server], rf.matchIndex[server])
			// 完全没有共识日志时的停止点
			if rf.nextIndex[server] == EmptyCmdIdx {
				rf.nextIndex[server] = EmptyCmdIdx + 1
				rf.matchIndex[server] = EmptyCmdIdx
				//DPrintf("leader=%d sync match index from follower=%d fail\n", rf.id, server)
			} /*else if rf.snapshot[0].SnapshotIndex >= reply.NextIndex {
				rf.SendInstallSnapshot(server)
				break
			}*/
			rf.RLock()
			if !(rf.nextIndex[server] == EmptyCmdIdx) && rf.snapshot[0].SnapshotIndex >= reply.NextIndex {
				rf.SendInstallSnapshot(server)
				break
			}
			// 快速同步
			continue
		}
	}
}

func (rf *Raft) SendInstallSnapshot(server int) {
	stop := time.NewTimer(RPCTimeOut * time.Millisecond)
	if rf.role != LEADER {
		stop.Stop()
		return
	}
	successChan := make(chan bool, 1)
	// TODO 考虑分成多次传输，防止心跳超时
	lastIncludedIndex, lastIncludeTerm := rf.snapshot[0].SnapshotIndex, rf.snapshot[0].SnapshotTerm
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludeTerm:   lastIncludeTerm,
		Offset:            0,
		Data:              rf.snapshot[0].Snapshot,
		Done:              true,
	}
	rf.RUnlock()
	reply := InstallSnapshotReply{}
	go func() {
		DPrintf("leader=%d发送快照to follower=%d\n", rf.id, server)
		res := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		successChan <- res
	}()
	stop.Stop()
	stop.Reset(RPCTimeOut * time.Millisecond)
	success := false
	select {
	case <-stop.C:
	case success = <-successChan:
	}
	if !success {
		DPrintf("leader=%d send installSnapshot RPC to follower=%d fail\n", rf.id, server)
	} else if reply.Term > rf.currentTerm {
		rf.changeRole(FOLLOWER)
	} else {
		rf.nextIndex[server] = lastIncludedIndex + 1
		rf.matchIndex[server] = lastIncludedIndex
	}
	rf.RLock()
}

// 提交日志
func (rf *Raft) updateCommitEntries(leaderCommit int) {
	// 获取最后一条日志的索引
	lastIndex := rf.lastLogIndex()
	originCommitIndex := rf.commitIndex
	rf.commitIndex = min(leaderCommit, lastIndex)
	if originCommitIndex != rf.commitIndex {
		//DPrintf("is time to refresh commit for follower=%d, originCommitIndex=%d, newCommitIndex=%d, leaderCommit=%d, lastIndex=%d\n",
		//	rf.id, originCommitIndex, rf.commitIndex, leaderCommit, lastIndex)
		if originCommitIndex != rf.commitIndex {
			rf.applyLogAsyncNow()
		}
	}
}

func (rf *Raft) findLastTermLogIdx(cmdIdx int) int {
	idx := rf.GenLogIdx(cmdIdx)
	if idx == -1 {
		return -1
	}
	term := rf.log[idx].ReceiveTerm
	ans := cmdIdx - 1
	for ; ans > rf.commitIndex && (rf.snapshot[0].SnapshotValid && rf.snapshot[0].SnapshotIndex < ans); ans-- {
		if rf.log[rf.GenLogIdx(ans)].ReceiveTerm < term {
			break
		}
	}
	return ans
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果任期比当前的小，但是commitIndex比当前节点大，说明可能当前节点可能曾经失去过联系
	if args.Term < rf.currentTerm {
		// 由于此处并不是leader发送的心跳，所以不用重置心跳超时定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 处理心跳
	//DPrintf("args=%v\n", args)
	rf.electionTimer.Stop()
	if rf.role == LEADER {
		DPrintf("server=%d change to follower, args.term=%d, curTerm=%d\n", rf.id, args.Term, rf.currentTerm)
	}
	// 包含了重置心跳超时定时器
	rf.changeRole(FOLLOWER)
	rf.currentTerm = args.Term
	rf.leader = args.LeaderId
	reply.Term = rf.currentTerm
	rf.Lock()
	defer func() {
		rf.persist()
		rf.Unlock()
	}()
	if args.Entries == nil {
		if args.PrevLogIndex == EmptyCmdIdx {
			reply.Success = true
			//DPrintf("heart beat handle success, leader=%d, me=%d, term=%d, myCommit=%d, time=%d\n",
			//	rf.leader, rf.id, rf.currentTerm, rf.commitIndex, time.Now().UnixMilli())
			return
		}
		logIndex := rf.lastLogIndex()
		if logIndex < args.PrevLogIndex {
			reply.Success = false
			reply.NextIndex = logIndex + 1
			return
		} else if rf.snapshot[0].SnapshotValid && logIndex == rf.snapshot[0].SnapshotIndex {
			reply.Success = true
			rf.updateCommitEntries(args.LeaderCommit)
			return
		} else if rf.snapshot[0].SnapshotValid && args.PrevLogIndex < rf.snapshot[0].SnapshotIndex {
			reply.Success = false
			reply.NextIndex = rf.snapshot[0].SnapshotIndex + 1
			rf.log = rf.log[:0]
			return
		} else if rf.snapshot[0].SnapshotValid && args.PrevLogIndex == rf.snapshot[0].SnapshotIndex {
			reply.Success = true
			rf.log = rf.log[:0]
			return
		}
		msg := &rf.log[rf.GenLogIdx(args.PrevLogIndex)]
		if msg.ReceiveTerm != args.PrevLogTerm || msg.CommandIndex != args.PrevLogIndex {
			reply.Success = false
			reply.NextIndex = rf.findLastTermLogIdx(args.PrevLogIndex) + 1
			//DPrintf("follower=%d's logs are diff from leader, need to sync\n", rf.id)
			return
		}
		rf.log = rf.log[:rf.GenLogIdx(args.PrevLogIndex)+1]
		reply.Success = true
		// 更新已提交的条目
		rf.updateCommitEntries(args.LeaderCommit)
		//DPrintf("heart beat handle success, leader=%d, me=%d, term=%d, myCommit=%d, time=%d\n",
		//	rf.leader, rf.id, rf.currentTerm, rf.commitIndex, time.Now().UnixMilli())
		return
	}
	//DPrintf("follower=%d handle logs, curTerm=%d, args=%v\n", rf.id, rf.currentTerm, args)
	if len(args.Entries) > 0 {
		DPrintf("follower=%d will handle count=%d, curIndex=%d\n", rf.id, len(args.Entries), rf.lastLogIndex())
		defer func(count int) {
			DPrintf("follower=%d handle count=%d finished.\n", rf.id, count)
		}(len(args.Entries))
	}
	if len(rf.log) == 0 {
		if rf.snapshot[0].SnapshotValid && args.PrevLogIndex == rf.snapshot[0].SnapshotIndex && args.PrevLogTerm == rf.snapshot[0].SnapshotTerm {
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			rf.updateCommitEntries(args.LeaderCommit)
		} else if !rf.snapshot[0].SnapshotValid && args.PrevLogTerm == EmptyCmdTerm && args.PrevLogIndex == EmptyCmdIdx {
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			rf.updateCommitEntries(args.LeaderCommit)
		} else {
			reply.Success = false
			reply.NextIndex = 1
		}
		return
	}
	// 追加日志
	prevLogIndex := args.PrevLogIndex
	if rf.lastLogIndex() < prevLogIndex {
		// 这条消息对于当前节点来说过于超前
		reply.Success = false
		reply.NextIndex = rf.findLastTermLogIdx(rf.lastLogIndex()) + 1
		//DPrintf("日志对于follower=%d过于超前\n", rf.id)
		return
	} else if prevLogIndex < rf.commitIndex {
		reply.Success = false
		reply.NextIndex = rf.commitIndex + 1
		return
	}
	// 寻找index相同的日志，并检查是否为共识日志
	index := rf.GenLogIdx(args.PrevLogIndex)
	if index == -1 {
		// 找不到共识日志, 并且prevLogIndex为快照的index
		rf.log = rf.log[:0]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		//DPrintf("follower=%d sync hole leader=%d's log\n", rf.id, args.LeaderId)
		rf.updateCommitEntries(args.LeaderCommit)
		return
	}
	logMsg := rf.log[index]
	if logMsg.ReceiveTerm != args.PrevLogTerm {
		// 非共识日志
		rf.log = rf.log[:index]
		reply.Success = false
		reply.NextIndex = index
	} else {
		// 对非共识日志进行丢弃后再进行追加共识日志
		rf.log = rf.log[:index+1]
		rf.log = append(rf.log, args.Entries...)
		// 更新日志提交信息
		rf.updateCommitEntries(args.LeaderCommit)
		reply.Success = true
		//DPrintf("follower=%d receive log, firstTerm=%d, firstIndex=%d, lastTerm=%d, lastIndex=%d\n",
		//	rf.id, args.Entries[0].ReceiveTerm, args.Entries[0].CommandIndex,
		//	args.Entries[len(args.Entries)-1].ReceiveTerm, args.Entries[len(args.Entries)-1].CommandIndex)
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.role == LEADER
	// Your code here (2B).
	if isLeader {
		rf.Lock()
		defer func() {
			rf.Unlock()
		}()
		if isLeader {
			term = rf.currentTerm
			index = rf.lastLogIndex()
			index++
			rf.log = append(rf.log, ApplyMsg{
				CommandValid: true,
				ReceiveTerm:  rf.currentTerm,
				Command:      command,
				CommandIndex: index,
			})
			rf.matchIndex[rf.me] = index
			rf.persist()
			DPrintf("leader=%d receive one log, logIndex=%d, curTerm=%d, log=%v\n", rf.id, index, rf.currentTerm, rf.log[len(rf.log)-1])
		}
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) fastNotifyLeaderChangeAndRegisterHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 采取的心跳注册策略为通知leader变更后，再进行注册心跳
		go func(server int) {
			rf.RLock()
			rf.SendAppend(server, 0)
			rf.RUnlock()
			rf.heartBeatTimer[server].Stop()
			rf.heartBeatTimer[server].Reset(OneHeartBeatLag * time.Millisecond)
			go func(index int) {
				for !rf.killed() && rf.role == LEADER {
					select {
					case <-rf.heartBeatTimer[index].C:
						rf.RLock()
						if rf.snapshot[0].SnapshotValid && rf.snapshot[0].SnapshotIndex >= rf.nextIndex[index] {
							rf.SendInstallSnapshot(index)
						} else {
							rf.SendAppend(index, len(rf.log))
						}
						rf.RUnlock()
					}
					rf.heartBeatTimer[index].Stop()
					rf.heartBeatTimer[index].Reset(OneHeartBeatLag * time.Millisecond)
				}
				rf.heartBeatTimer[index].Stop()
			}(server)
		}(i)
	}
}

func (rf *Raft) updateCommit() {
	if rf.lastLogIndex() > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		i := rf.commitIndex + 1
		if rf.snapshot[0].SnapshotValid && rf.snapshot[0].SnapshotIndex >= i {
			i = rf.snapshot[0].SnapshotIndex + 1
		}
		for ; i <= rf.lastLogIndex(); i++ {
			// 如raft论文所讲，非当前任期的日志，不能由当前的leader进行commit，而是通过提交当前任期的日志来进行间接的提交
			// 但这种做法是有问题的，例如: 宕机后选举出leader后任期就不同了，然后这条日志之后也没有新日志的加入，就会导致永远不会被提交
			// 因为永远不会被提交，所以也不会被状态机应用
			// 解决这个问题可以研究一下no-op，在6.824中并不讨论这个问题
			if rf.log[rf.GenLogIdx(i)].ReceiveTerm != rf.currentTerm {
				continue
			}
			receive := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= i {
					receive++
				}
			}
			if receive <= len(rf.peers)/2 {
				//DPrintf("leader=%d log=%d未达成共识\n", rf.id, i)
				break
			}
			rf.commitIndex = i
			//DPrintf("leader=%d 发现logIndex=%d达成了共识, matchIndex=%v\n", rf.id, rf.commitIndex, rf.matchIndex)
		}
		if originCommitIndex != rf.commitIndex {
			rf.applyLogAsyncNow()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		success := false
		select {
		case <-rf.electionTimer.C:
			{
				//DPrintf("选举超时, id=%d, time=%d\n", rf.id, time.Now().UnixMilli())
				success = rf.startElect(false)
			}
		case <-rf.waitHeartBeatTimer.C:
			{
				if rf.role == FOLLOWER {
					rf.leader = VoteForInvalid
					DPrintf("心跳超时, id=%d, time=%d\n", rf.id, time.Now().UnixMilli())
					success = rf.startElect(true)
				}
			}
		}
		if success {
			//DPrintf("id=%d success to elect leader\n", rf.id)
		}
		//DPrintf("id=%d finish to elect leader\n", rf.id)
	}
}

func (rf *Raft) resetWaitHeartBeatTimer() {
	rf.waitHeartBeatTimer.Stop()
	rf.waitHeartBeatTimer.Reset(time.Duration(HeartBeatTimeOut+rand.Int63()%100) * time.Millisecond)
}

func (rf *Raft) resetElectTimeOut() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomElectTimeOut())
}

func (rf *Raft) applyLogAsyncNow() {
	rf.applyLogTimer.Stop()
	rf.applyLogTimer.Reset(0)
}

func (rf *Raft) apply(msg *ApplyMsg) bool {
	select {
	case rf.applyCh <- *msg:
		return true
	default:
		timeout := time.NewTimer(ApplyWaitTimeOut)
		select {
		case rf.applyCh <- *msg:
			return true
		case <-timeout.C:
			return false
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 服务或测试者想要创建一个 Raft 服务器。所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组的顺序相同。
// persister 是这个服务器用于保存其持久状态的地方，并在开始时保存最近保存的状态（如果有的话）。
// applyCh 是一个通道，测试者或服务期望 Raft 在上面发送 ApplyMsg 消息。
// Make() 必须迅速返回，因此它应该为任何长时间运行的工作启动 goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = FOLLOWER
	rf.leader = VoteForInvalid
	rf.voteFor = VoteForInvalid
	rf.id = me
	rf.log = []ApplyMsg{}
	rf.snapshot = make([]*ApplyMsg, 2)
	for i := range rf.snapshot {
		rf.snapshot[i] = new(ApplyMsg)
		snapshotInit(rf.snapshot[i], false, EmptyCmdTerm, EmptyCmdIdx, nil)
	}
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.waitHeartBeatTimer = time.NewTimer(time.Duration(HeartBeatTimeOut+rand.Int63()%100) * time.Millisecond)
	rf.electionTimer = time.NewTimer(randomElectTimeOut())
	rf.heartBeatTimer = make([]*time.Timer, len(peers))
	for i := range rf.heartBeatTimer {
		rf.heartBeatTimer[i] = time.NewTimer(OneHeartBeatLag * time.Millisecond)
	}
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("make an entry id=%s\n", strconv.Itoa(me))

	// start ticker goroutine to start elections
	go rf.ticker()

	// 在这里初始化，防止其他地方调用AsyncNow时，applyLogTimer未初始化
	rf.applyLogTimer = time.NewTimer(0)
	// 注册日志应用到状态机的任务
	go func() {
		for {
			select {
			case <-rf.applyLogTimer.C:
				//DPrintf("server=%d start to apply log. lastApply=%d, commitIndex=%d\n", rf.id, rf.lastApplied, rf.commitIndex)
				//originApply := rf.lastApplied
				// 如果rf.applyCh未来得及消费，则停止apply，避免死锁
				rf.RLock()
				// 先将快照进行apply
				success := true
				if rf.snapshot[0].SnapshotValid && rf.lastApplied < rf.snapshot[0].SnapshotIndex {
					success = rf.apply(rf.snapshot[0])
					if success {
						rf.lastApplied = rf.snapshot[0].SnapshotIndex
						DPrintf("server=%d apply snapshot, index=%d, term=%d\n", rf.id, rf.snapshot[0].SnapshotIndex, rf.snapshot[0].SnapshotTerm)
					} else {
						rf.applyLogAsyncNow()
					}
				}
				// 对非快照进行apply
				for success && rf.lastApplied < rf.commitIndex {
					applyIdx := rf.GenLogIdx(rf.lastApplied + 1)
					success = rf.apply(&rf.log[applyIdx])
					if success {
						rf.lastApplied++
						DPrintf("server=%d apply log=%v\n", rf.id, rf.log[applyIdx])
					} else {
						rf.applyLogAsyncNow()
						break
					}
				}
				rf.RUnlock()
			}
			//rf.applyLogTimer.Stop()
			//rf.applyLogTimer.Reset(ApplyLogLag * time.Millisecond)
		}
	}()

	return rf
}
