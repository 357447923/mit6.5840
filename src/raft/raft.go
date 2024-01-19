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
	"fmt"
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
	OneHeartBeatLag  = 100
	UpdateCommitLag  = 200
	ApplyLogLag      = 200
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

	id                 int  // 当前节点的id
	leader             int  // 当前集群的leader
	role               Role // 当前节点的角色
	electionTimer      *time.Timer
	heartBeatTimer     []*time.Timer
	waitHeartBeatTimer *time.Timer
	commitUpdateTimer  *time.Timer
	applyLogTimer      *time.Timer

	// exists stably in all server as follows（持久数据）
	currentTerm int        // last Term of server knowing
	voteFor     int        // 当前任期内收到选票的Candidate id（没有就为-1）
	log         []ApplyMsg // 每个条目包含状态机的要执行命令和从 Leader 处收到时的任期号, 0这个位置可能要丢弃

	// exists unstably in all server as follows(非持久数据)
	commitIndex int // 已知的被提交的最大日志条目的索引值（从0开始递增）
	lastApplied int // 被状态机执行的最大日志条目的索引值（从0开始递增）

	// exists unstably in leader server as follows(非持久数据)
	nextIndex  []int // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为 Leader 上一条日志的索引值+1）
	matchIndex []int // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
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
			//rf.registerUpdateLeaderCommit()
		}
	case FOLLOWER:
		{
			if rf.role == LEADER {
				DPrintf("id=%d change to follower from leader\n", rf.id)
			}
			rf.role = FOLLOWER
			//rf.nextIndex = nil
			//rf.matchIndex = nil
			rf.resetWaitHeartBeatTimer()
		}
	default:
		panic("unknown role")
	}
}

func randomElectTimeOut() time.Duration {
	return time.Duration(BaseElectTimeOut+rand.Int63()%BaseElectTimeOut) * time.Millisecond
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
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	// TODO 2D:需要把上一次快照的位置进行持久化
	e.Encode(rf.log)
	raftState := buf.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
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
		DPrintf("server=%d read data success! term=%d, voteFor=%d, commitIndex=%d\n", rf.id, rf.currentTerm, rf.voteFor, rf.commitIndex)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		DPrintf("server=%d, currentTerm=%d >= args.Term=%d, reject to vote to server=%d\n", rf.id, rf.currentTerm, args.Term, args.CandidateId)
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
	originTerm := rf.currentTerm
	if args.Term > rf.currentTerm {
		// 可能是leader断网之后，发现其他节点正在选举
		if rf.role == LEADER {
			DPrintf("server=%d rf.currentTerm=%d, args.Term=%d\n", rf.id, rf.currentTerm, args.Term)
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
	DPrintf("server=%d currentTerm=%d, originTerm=%d, lastLogIndex=%d, lastLogTerm=%d, args.PrevLogTerm=%d, args.PrevLogIndex=%d, vote server=%d\n",
		rf.id, rf.currentTerm, originTerm, lastLogIndex, lastLogTerm, args.PrevLogTerm, args.PrevLogIndex, args.CandidateId)

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
	// TODO 2D时要考虑可能是更新了快照导致日志长度为0
	if len(rf.log) == 0 {
		return EmptyCmdIdx
	} else {
		return rf.log[len(rf.log)-1].CommandIndex
	}
}

func (rf *Raft) lastLogTerm() int {
	// TODO 2D时要考虑可能是更新了快照导致日志长度为0
	if len(rf.log) == 0 {
		return EmptyCmdTerm
	} else {
		return rf.log[len(rf.log)-1].ReceiveTerm
	}
}

func (rf *Raft) lastLogTermIndex() (prevLogTerm int, prevLogIndex int) {
	// 集群刚启动时就是没有日志的
	// TODO 2D时要考虑可能是更新了快照导致日志长度为0
	if len(rf.log) == 0 {
		prevLogIndex = EmptyCmdIdx
		prevLogTerm = EmptyCmdTerm
	} else {
		prevLogIndex = rf.log[len(rf.log)-1].CommandIndex
		prevLogTerm = rf.log[GenLogIdx(prevLogIndex)].ReceiveTerm
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
		DPrintf("id=%d get vote less than 1/2 votes. grantedCount=%d\n", rf.id, voteCount)
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
	// TODO 当做到快照的时候需要修改
	if len(rf.log) == 0 {
		args.Entries = nil
		args.PrevLogTerm, args.PrevLogIndex = EmptyCmdTerm, EmptyCmdIdx
	} else if len(rf.log) >= rf.nextIndex[server] {
		if matchIndex == EmptyCmdIdx {
			DPrintf("follower=%d's match index=0 in leader=%d's list\n", server, rf.id)
			args.PrevLogTerm, args.PrevLogIndex = EmptyCmdTerm, EmptyCmdIdx
		} else {
			prevCommand := rf.log[GenLogIdx(matchIndex)]
			args.PrevLogIndex = prevCommand.CommandIndex
			args.PrevLogTerm = prevCommand.ReceiveTerm
		}
		DPrintf("genArgs nextIndex=%d, sender=%d, receiver=%d, maxLogCount=%d\n", rf.nextIndex[server], rf.id, server, maxLogCount)
		if maxLogCount > 0 {
			idx := GenLogIdx(rf.nextIndex[server])
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
	DPrintf("genArgs result=%v. It's leader=%d sending to follower=%d\n", args, rf.id, server)
	return args
}

func (rf *Raft) SendAppend(server int, maxAppendCount int) {
	stop := time.NewTimer(RPCTimeOut * time.Millisecond)
	for {
		if rf.role != LEADER {
			break
		}
		args := rf.genAppendEntriesArgs(server, maxAppendCount)
		var reply AppendEntriesReply
		successChan := make(chan bool, 1)
		success := false
		stop.Stop()
		stop.Reset(RPCTimeOut * time.Millisecond)
		go func() {
			DPrintf("leader=%d发送心跳to follower=%d\n", rf.id, server)
			res := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			successChan <- res
		}()
		select {
		case <-stop.C:
			{
				break
			}
		case success = <-successChan:
			break
		}
		if !success {
			DPrintf("leader=%d send appendEntries RPC to follower=%d fail\n", rf.id, server)
			continue
		} else if reply.Success {
			// 成功并且有发送日志，则需要更新matchIndex和nextIndex
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].CommandIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("leader=%d update follower=%d's match index to %d\n", rf.id, server, rf.matchIndex[server])
				if args.Entries[len(args.Entries)-1].ReceiveTerm == rf.currentTerm {
					rf.updateCommit()
					rf.persist()
				}
			}
			break
			// 收到的回应任期比当前任期大，说明当前节点不是leader
		} else if reply.Term > rf.currentTerm {
			rf.changeRole(FOLLOWER)
			break
		} else {
			rf.nextIndex[server] = reply.NextIndex
			rf.matchIndex[server] = reply.NextIndex - 1
			// 日志非共识日志，寻找共识日志
			//rf.nextIndex[server]--
			//rf.matchIndex[server]--
			DPrintf("no find follower=%d's match log, nextIndex=%d, matchIndex=%d\n", server, rf.nextIndex[server], rf.matchIndex[server])
			// 完全没有共识日志时的停止点
			if rf.nextIndex[server] == EmptyCmdIdx {
				rf.nextIndex[server] = EmptyCmdIdx + 1
				rf.matchIndex[server] = EmptyCmdIdx
				DPrintf("leader=%d sync match index from follower=%d fail\n", rf.id, server)
				break
			}
			// fast send heart beat or fast sync log
			//rf.heartBeatTimer[server].Stop()
			//rf.heartBeatTimer[server].Reset(0)
		}
	}
}

// 提交日志
func (rf *Raft) updateCommitEntries(leaderCommit int) {
	// 获取最后一条日志的索引
	lastIndex := rf.lastLogIndex()
	originCommitIndex := rf.commitIndex
	rf.commitIndex = min(leaderCommit, lastIndex)
	if originCommitIndex != rf.commitIndex {
		DPrintf("is time to refresh commit for follower=%d, originCommitIndex=%d, newCommitIndex=%d, leaderCommit=%d, lastIndex=%d\n",
			rf.id, originCommitIndex, rf.commitIndex, leaderCommit, lastIndex)
		if originCommitIndex != rf.commitIndex {
			rf.applyLogAsyncNow()
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果任期比当前的小，但是commitIndex比当前节点大，说明可能当前节点可能曾经失去过联系
	//if args.Term < rf.currentTerm && args.LeaderCommit <= rf.commitIndex {
	if args.Term < rf.currentTerm {
		// 由于此处并不是leader发送的心跳，所以不用重置心跳超时定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 处理心跳
	DPrintf("args=%v\n", args)
	rf.electionTimer.Stop()
	if rf.role == LEADER {
		DPrintf("server=%d change to follower, args.term=%d, curTerm=%d\n", rf.id, args.Term, rf.currentTerm)
	}
	// 包含了重置心跳超时定时器
	rf.changeRole(FOLLOWER)
	rf.currentTerm = args.Term
	rf.leader = args.LeaderId
	reply.Term = rf.currentTerm
	defer rf.persist()
	if args.Entries == nil {
		if args.PrevLogIndex == EmptyCmdIdx {
			reply.Success = true
			DPrintf("heart beat handle success, leader=%d, me=%d, term=%d, myCommit=%d, time=%d\n",
				rf.leader, rf.id, rf.currentTerm, rf.commitIndex, time.Now().UnixMilli())
			return
		}
		logIndex := rf.lastLogIndex()
		if logIndex < args.PrevLogIndex {
			reply.Success = false
			reply.NextIndex = logIndex + 1
			return
		}
		msg := &rf.log[GenLogIdx(args.PrevLogIndex)]
		if msg.ReceiveTerm != args.PrevLogTerm || msg.CommandIndex != args.PrevLogIndex {
			reply.Success = false
			reply.NextIndex = rf.commitIndex + 1
			DPrintf("follower=%d's logs are diff from leader, need to sync\n", rf.id)
			return
		}
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = true
		// 更新已提交的条目
		rf.updateCommitEntries(args.LeaderCommit)
		DPrintf("heart beat handle success, leader=%d, me=%d, term=%d, myCommit=%d, time=%d\n",
			rf.leader, rf.id, rf.currentTerm, rf.commitIndex, time.Now().UnixMilli())
		return
	}
	DPrintf("follower=%d handle logs, curTerm=%d, args=%v\n", rf.id, rf.currentTerm, args)
	if len(args.Entries) > 0 {
		DPrintf("follower=%d will handle count=%d, curIndex=%d\n", rf.id, len(args.Entries), rf.lastLogIndex())
	}
	if len(rf.log) == 0 {
		if args.PrevLogTerm == EmptyCmdTerm && args.PrevLogIndex == EmptyCmdIdx {
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
	msg := rf.log[len(rf.log)-1]
	if msg.CommandIndex < prevLogIndex {
		// 这条消息对于当前节点来说过于超前
		reply.Success = false
		reply.NextIndex = rf.lastLogIndex() + 1
		return
	}
	// 寻找index相同的日志，并检查是否为共识日志
	// TODO 当完成2D后应该使用被注释的部分
	/*index := GenLogIdx(rf.log)
	for ; index >= 0; index-- {
		logMsg := rf.log[index]
		if logMsg.CommandIndex == prevLogIndex {
			break
		}
	}*/
	index := GenLogIdx(args.PrevLogIndex)
	if index == -1 {
		// 找不到共识日志
		rf.log = rf.log[:0]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		DPrintf("follower=%d sync hole leader=%d's log\n", rf.id, args.LeaderId)
		rf.updateCommitEntries(args.LeaderCommit)
		return
	}
	logMsg := rf.log[index]
	if logMsg.ReceiveTerm != args.PrevLogTerm {
		// 非共识日志
		rf.log = rf.log[:index]
		reply.Success = false
		reply.NextIndex = index + 1
	} else {
		// 对非共识日志进行丢弃后再进行追加共识日志
		rf.log = rf.log[:index+1]
		rf.log = append(rf.log, args.Entries...)
		// 更新日志提交信息
		rf.updateCommitEntries(args.LeaderCommit)
		reply.Success = true
		DPrintf("follower=%d receive log, firstTerm=%d, firstIndex=%d, lastTerm=%d, lastIndex=%d\n",
			rf.id, args.Entries[0].ReceiveTerm, args.Entries[0].CommandIndex,
			args.Entries[len(args.Entries)-1].ReceiveTerm, args.Entries[len(args.Entries)-1].CommandIndex)
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
		rf.mu.Lock()
		defer rf.mu.Unlock()
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
			if len(rf.matchIndex) < len(rf.peers) {
				panic(fmt.Sprintf("leader=%d's matchIndexLen=%d, but expect len=%d, matchIndex=%v\n", rf.id, len(rf.matchIndex), len(rf.peers), rf.matchIndex))
			}
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
			pass := make(chan bool, 1)
			go func() {
				beforeNext := rf.nextIndex[server]
				rf.SendAppend(server, 0)
				// 快速同步
				for !rf.killed() && beforeNext > rf.nextIndex[server] && rf.role == LEADER {
					beforeNext = rf.nextIndex[server]
					rf.SendAppend(server, 0)
				}
				pass <- true
			}()
			select {
			case <-pass:
			}
			rf.heartBeatTimer[server].Stop()
			rf.heartBeatTimer[server].Reset(OneHeartBeatLag * time.Millisecond)
			go func(index int) {
				for !rf.killed() && rf.role == LEADER {
					select {
					case <-rf.heartBeatTimer[index].C:
						rf.SendAppend(index, len(rf.log))
					}
					rf.heartBeatTimer[index].Stop()
					rf.heartBeatTimer[index].Reset(OneHeartBeatLag * time.Millisecond)
					//DPrintf("leader对%d的心跳定时器重置成功, time=%d\n", index, time.Now().UnixMilli())
				}
				rf.heartBeatTimer[index].Stop()
			}(server)
		}(i)
	}
}

func (rf *Raft) registerUpdateLeaderCommit() {
	if rf.commitUpdateTimer == nil {
		rf.commitUpdateTimer = time.NewTimer(UpdateCommitLag * time.Millisecond)
	} else {
		rf.commitUpdateTimer.Stop()
		rf.commitUpdateTimer.Reset(UpdateCommitLag * time.Millisecond)
	}

	go func() {
		for rf.role == LEADER {
			select {
			case <-rf.commitUpdateTimer.C:
				{
					rf.updateCommit()
					rf.persist()
				}
			}
			rf.commitUpdateTimer.Stop()
			rf.commitUpdateTimer.Reset(OneHeartBeatLag * time.Millisecond)
		}
		rf.commitUpdateTimer.Stop()
	}()
}

func (rf *Raft) updateCommit() {
	// TODO 2D做快照后，这里都得修改
	if rf.lastLogIndex() > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		for i := rf.commitIndex + 1; i <= rf.lastLogIndex(); i++ {
			// 如raft论文所讲，非当前任期的日志，不能由当前的leader进行commit，而是通过提交当前任期的日志来进行间接的提交
			// 但这种做法是有问题的，例如: 宕机后选举出leader后任期就不同了，然后这条日志之后也没有新日志的加入，就会导致永远不会被提交
			// 因为永远不会被提交，所以也不会被状态机应用
			// 解决这个问题可以研究一下no-op，在6.824中并不讨论这个问题
			if rf.log[GenLogIdx(i)].ReceiveTerm != rf.currentTerm {
				continue
			}
			receive := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= i {
					receive++
				}
			}
			if receive <= len(rf.peers)/2 {
				DPrintf("leader=%d log=%d未达成共识\n", rf.id, i)
				break
			}
			rf.commitIndex = i
			DPrintf("leader=%d 发现logIndex=%d达成了共识, matchIndex=%v\n", rf.id, rf.commitIndex, rf.matchIndex)
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
		var success bool
		select {
		case <-rf.electionTimer.C:
			{
				DPrintf("选举超时, id=%d, time=%d\n", rf.id, time.Now().UnixMilli())
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
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
		//rf.applyLogTimer = time.NewTimer(0)
		for {
			select {
			case <-rf.applyLogTimer.C:
				{
					originApply := rf.lastApplied
					for rf.lastApplied < rf.commitIndex {
						rf.lastApplied++
						rf.applyCh <- rf.log[GenLogIdx(rf.lastApplied)]
						DPrintf("server=%d apply log=%v \n", rf.id, rf.log[GenLogIdx(rf.lastApplied)])
					}
					if rf.lastApplied > originApply {
						if rf.role == LEADER {
							DPrintf("leader=%d refresh commit success, apply log from index=%d to index=%d, rf.matchIndex=%v\n",
								rf.id, originApply, rf.lastApplied, rf.matchIndex)
						} else {
							DPrintf("follower=%d refresh commit success, apply log from index=%d to index=%d\n", rf.id, originApply, rf.lastApplied)
						}
					}
				}
			}
			rf.applyLogTimer.Stop()
			rf.applyLogTimer.Reset(ApplyLogLag * time.Millisecond)
		}
	}()

	return rf
}
