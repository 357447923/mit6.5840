package shardctrler

import (
	"6.5840/raft"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const WaitOpTimeOut = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	lastReq    map[int64]ReqId          // clientId:reqId
	notifyChan map[int64]chan NotifyMsg // clientId:chan notify
}

func (sc *ShardCtrler) lock() {
	sc.mu.Lock()
}

func (sc *ShardCtrler) unlock() {
	sc.mu.Unlock()
}

type ReqId int64
type Op struct {
	// Your data here.
	ClientId int64
	ReqId    ReqId
	Args     interface{}
	Method   string
}

type NotifyMsg struct {
	WrongLeader bool
	Err         Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

func (sc *ShardCtrler) getConfig(idx int) Config {
	if idx < 0 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}

	return sc.configs[idx]
}

func (sc *ShardCtrler) isRepeated(clientId int64, reqId ReqId) bool {
	if val, ok := sc.lastReq[clientId]; ok {
		return val == reqId
	}
	return false
}

func (sc *ShardCtrler) handleApplyCmd() {
	for {
		select {
		case msg := <-sc.applyCh:
			DPrintf("server=%d handle msg, log=%v\n", sc.me, msg)
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			sc.lock()
			isRepeated := sc.isRepeated(op.ClientId, op.ReqId)
			var err Err
			if !isRepeated {

			}
			if ch, ok := sc.notifyChan[op.ClientId]; ok {
				if len(ch) == 0 {
					ch <- NotifyMsg{
						WrongLeader: false,
						Err:         err,
					}
				}
				timeout := time.NewTimer(WaitOpTimeOut)
				select {
				case <-timeout.C:
					DPrintf("follower=%d say I'm timeout\n", sc.me)
				case ch <- NotifyMsg{
					WrongLeader: false,
					Err:         err,
				}:
				}
			}
			sc.unlock()
		}
	}
}

func (sc *ShardCtrler) handleJoin(args *JoinArgs) {
	conf := sc.getConfig(-1)
	conf.Num++
	for k, v := range args.Servers {
		conf.Groups[k] = v
	}

	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleLeave(args *LeaveArgs) {

}

func (sc *ShardCtrler) handleMove(args *MoveArgs) {

}

func (sc *ShardCtrler) handleQuery(args *QueryArgs, reply *QueryReply) {
	if args.Num >= len(sc.configs) || args.Num < 0 {
		reply.Err = ConfigNotExists
	}
	reply.Config = sc.getConfig(args.Num)
	reply.Err = OK
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
