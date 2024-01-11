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
	OneHeartBeatLag  = 200
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
	CommandIndex int // 在哪一任上收到消息
	ReceiveTerm  int

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
			rf.resetElectTimeOut()
		}
	case LEADER:
		{
			rf.role = LEADER
			logIndex := rf.lastLogIndex()
			DPrintf("leader=%d's lastLogIdx=%d, log=%v\n", rf.id, logIndex, rf.log)
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = logIndex + 1
			}
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.matchIndex {
				rf.matchIndex[i] = logIndex
			}
			// 注册commit更新任务
			rf.commitUpdateTimer = time.NewTimer(300 * time.Millisecond)
			go func() {
				for rf.role == LEADER {
					select {
					case <-rf.commitUpdateTimer.C:
						{
							rf.updateCommit()
						}
					}
					rf.commitUpdateTimer.Stop()
					rf.commitUpdateTimer.Reset(300 * time.Millisecond)
				}
			}()
			rf.electionTimer.Stop()
			rf.resetHeartBeatTimer()
			rf.registerHandleHeartBeat()
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

func (rf *Raft) resetHeartBeatTimer() {
	for _, timer := range rf.heartBeatTimer {
		timer.Stop()
		timer.Reset(OneHeartBeatLag * time.Millisecond)
	}
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	if args.Term < rf.currentTerm {
		reply.CurrTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 当前任期未投票，则需要投票
	if args.Term > rf.currentTerm {
		// 可能是leader断网之后，发现其他节点正在选举
		if rf.role == LEADER {
			DPrintf("rf.currentTerm=%d, args.Term=%d\n", rf.currentTerm, args.Term)
			rf.changeRole(FOLLOWER)
		}
		rf.resetElectTimeOut()
		rf.voteFor = -1
	}
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if len(rf.log) == 0 {
			rf.voteFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.CurrTerm = args.Term
			reply.VoteGranted = true
			DPrintf("id=%d vote to id=%d, term=%d\n", rf.id, rf.voteFor, rf.currentTerm)
			return
		}
		log := rf.log[len(rf.log)-1]
		if args.PrevLogTerm >= log.ReceiveTerm && args.PrevLogIndex >= log.CommandIndex {
			reply.CurrTerm = args.Term
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			DPrintf("id=%d vote to id=%d, term=%d\n", rf.id, rf.voteFor, rf.currentTerm)
		}

	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.voteFor = rf.id
	voteCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch *chan bool, index int) {
			var reply RequestVoteReply
			DPrintf("id=%d send to id=%d\n", rf.id, index)
			end := false
			for !end {
				end = rf.sendRequestVote(index, &args, &reply)
			}
			votesCh <- reply.VoteGranted
			if reply.CurrTerm > args.Term {
				rf.currentTerm = reply.CurrTerm
				rf.changeRole(FOLLOWER)
			}
		}(&votesCh, index)
	}
	end := false
	for !end {
		select {
		case <-rf.electionTimer.C:
			{
				rf.changeRole(FOLLOWER)
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
		rf.changeRole(LEADER)
		DPrintf("id=%d become leader\n", rf.id)
		rf.SendHeartBeatAsync()
	} else {
		rf.changeRole(FOLLOWER)
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
	Term    int  // 当前任期号
	Success bool // append操作被接受了
}

func min(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) genAppendEntriesArgs(server int) AppendEntriesArgs {
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
			args.PrevLogTerm, args.PrevLogIndex = EmptyCmdTerm, EmptyCmdIdx
		} else {
			prevCommand := rf.log[GenLogIdx(matchIndex)]
			args.PrevLogIndex = prevCommand.CommandIndex
			args.PrevLogTerm = prevCommand.ReceiveTerm
		}
		DPrintf("genArgs nextIndex=%d, receiver=%d\n", rf.nextIndex[server], server)
		args.Entries = rf.log[GenLogIdx(rf.nextIndex[server]):]
	} else {
		// 用于同步matchIndex和nextIndex
		args.Entries = nil
		args.PrevLogTerm, args.PrevLogIndex = rf.lastLogTermIndex()
	}
	return args
}

func (rf *Raft) SendAppend(server int) {
	stop := time.NewTimer(OneHeartBeatLag * time.Millisecond)
	for {
		args := rf.genAppendEntriesArgs(server)
		var reply AppendEntriesReply
		successChan := make(chan bool, 1)
		success := false
		go func() {
			success = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			successChan <- success
		}()
		stop.Stop()
		stop.Reset(OneHeartBeatLag * time.Millisecond)
		select {
		case <-stop.C:
			{
				break
			}
		case <-successChan:
			break
		}
		rf.heartBeatTimer[server].Stop()
		rf.heartBeatTimer[server].Reset(OneHeartBeatLag * time.Millisecond)
		if !success {
			DPrintf("leader=%d send appendEntries RPC to follower=%d fail\n", rf.id, server)
			break
		} else if reply.Success {
			// 成功并且有发送日志，则需要更新matchIndex和nextIndex
			if len(args.Entries) != 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].CommandIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("follower=%d update match index to %d\n", server, rf.matchIndex[server])
			}
			break
			// 有点奇怪，我以为这里应该是收到的回应任期比当前任期大，说明当前节点不是leader，但是似乎不是这样
			// TODO 看懂他并且写注释
			// 我先试试看修改成我上面的理解有没有问题，原来的(reply.Term < rf.currentTerm)
		} else if reply.Term > rf.currentTerm {
			rf.changeRole(FOLLOWER)
			break
		} else {
			// 日志非共识日志，寻找共识日志
			rf.nextIndex[server]--
			rf.matchIndex[server]--
			// 完全没有共识日志时的停止点
			if rf.nextIndex[server] == EmptyCmdIdx {
				rf.nextIndex[server] = EmptyCmdIdx + 1
				rf.matchIndex[server] = EmptyCmdIdx
				DPrintf("leader=%d sync match index from follower=%d fail after becoming leader\n", rf.id, server)
				break
			}
		}
	}
}

func (rf *Raft) SendHeartBeatAsync() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			DPrintf("leader=%d send heart beat to %d\n", rf.id, index)
			rf.SendAppend(index)
		}(index)
	}
}

// 提交日志
func (rf *Raft) updateCommitEntries(leaderCommit int) {
	// 获取最后一条日志的索引
	lastIndex := rf.lastLogIndex()
	originCommitIndex := rf.commitIndex
	rf.commitIndex = min(leaderCommit, lastIndex)
	newlyCommit := []int{}
	if originCommitIndex != rf.commitIndex {
		DPrintf("is time to refresh commit for follower=%d\n", rf.id)
	}
	for i := originCommitIndex + 1; i <= rf.commitIndex; i++ {
		newlyCommit = append(newlyCommit, i)
		//DPrintf("follower=%d refresh commit success, put log=%v\n", rf.id, rf.log[GenLogIdx(i)])
		rf.applyCh <- rf.log[GenLogIdx(i)]
	}
	if len(newlyCommit) > 0 {
		DPrintf("follower=%d refresh commit success, put logIndex=%v\n", rf.id, newlyCommit)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		// 由于此处并不是leader发送的心跳，所以不用重置心跳超时定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 处理心跳
	DPrintf("args=%v\n", args)
	if args.Entries == nil {
		rf.electionTimer.Stop()
		// 包含了重置心跳超时定时器
		rf.changeRole(FOLLOWER)
		rf.currentTerm = args.Term
		rf.leader = args.LeaderId
		reply.Term = rf.currentTerm
		term, logIndex := rf.lastLogTermIndex()
		// 日志不同，需要做同步操作
		if args.PrevLogTerm != term || args.PrevLogIndex != logIndex {
			reply.Success = false
			DPrintf("logs are diff from leader, need to sync\n")
			return
		}
		reply.Success = true
		// 更新已提交的条目
		rf.updateCommitEntries(args.LeaderCommit)
		DPrintf("heart beat handle success, leader=%d, me=%d, term=%d, myCommit=%d, time=%d, logs=%v\n",
			rf.leader, rf.id, rf.currentTerm, rf.commitIndex, time.Now().UnixMilli(), rf.log)
		return
	}
	DPrintf("follower=%d handle logs, args=%v\n", rf.id, args)
	if len(args.Entries) > 0 {
		DPrintf("follower=%d handle count=%d\n", rf.id, len(args.Entries))
	}
	// TODO 貌似目前此处没有处理成功
	// TODO 貌似是leader无法判断是否发送的是相同的日志导致的错误
	if len(rf.log) == 0 {
		if args.PrevLogTerm == EmptyCmdTerm && args.PrevLogIndex == EmptyCmdIdx {
			rf.log = append(rf.log, args.Entries...)
			reply.Term = rf.currentTerm
			reply.Success = true
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
		return
	}
	// TODO 似乎日志重复会返回false，解决这个问题
	// 追加日志
	prevLogIndex := args.PrevLogIndex
	msg := rf.log[len(rf.log)-1]
	if msg.CommandIndex < prevLogIndex {
		// 这条消息对于当前节点来说过于超前
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("这条消息对于节点%d过于超前\n", rf.id)
		return
	}
	// 寻找index相同的日志，并检查是否为共识日志
	index := len(rf.log) - 1
	for ; index >= 0; index-- {
		logMsg := rf.log[index]
		if logMsg.CommandIndex == prevLogIndex {
			break
		}
	}
	if index == -1 {
		// 找不到共识日志
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("节点%d未找到与leader的共识日志", rf.id)
		return
	}
	logMsg := rf.log[index]
	if logMsg.ReceiveTerm != args.PrevLogTerm {
		// 非共识日志
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// 对非共识日志进行丢弃后再进行追加共识日志
		rf.log = rf.log[:index+1]
		rf.log = append(rf.log, args.Entries...)
		// 更新日志提交信息
		rf.updateCommitEntries(args.LeaderCommit)
		reply.Term = rf.currentTerm
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
	rf.voteFor = VoteForInvalid
	// Your code here (2B).
	if isLeader {
		term = rf.currentTerm
		rf.mu.Lock()
		index = rf.lastLogIndex()
		index++
		rf.log = append(rf.log, ApplyMsg{
			CommandValid: true,
			ReceiveTerm:  rf.currentTerm,
			Command:      command,
			CommandIndex: index,
		})
		rf.matchIndex[rf.me] = index
		rf.mu.Unlock()
		rf.persist()
		DPrintf("leader=%d receive one log, logIndex=%d\n", rf.id, index)
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

func (rf *Raft) registerHandleHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for rf.role == LEADER {
				select {
				case <-rf.heartBeatTimer[index].C:
					{
						rf.SendAppend(index)
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommit() {
	// TODO 2D做快照后，这里都得修改
	if rf.lastLogIndex() > rf.commitIndex {
		newlyCommit := []int{}
		for i := rf.commitIndex + 1; i <= rf.lastLogIndex(); i++ {
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
			rf.applyCh <- rf.log[GenLogIdx(i)]
			newlyCommit = append(newlyCommit, i)
			rf.commitIndex++
		}
		if len(newlyCommit) > 0 {
			DPrintf("leader=%d refresh commit success, put logIndex=%v, rf.matchIndex=%v\n",
				rf.id, newlyCommit, rf.matchIndex)
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
			DPrintf("id=%d success to elect leader\n", rf.id)
		}
		DPrintf("id=%d finish to elect leader\n", rf.id)
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
	rf.voteFor = -1
	rf.id = me
	rf.log = []ApplyMsg{}
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.waitHeartBeatTimer = time.NewTimer(time.Duration(HeartBeatTimeOut+rand.Int63()%100) * time.Millisecond)
	rf.electionTimer = time.NewTimer(randomElectTimeOut())
	rf.heartBeatTimer = make([]*time.Timer, len(peers))
	for i := range rf.heartBeatTimer {
		rf.heartBeatTimer[i] = time.NewTimer(OneHeartBeatLag * time.Millisecond)
	}
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("make an entry id=%s\n", strconv.Itoa(me))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
