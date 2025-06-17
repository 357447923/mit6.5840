package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const WaitOpTimeOut = 500 * time.Millisecond

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

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

func (sc *ShardCtrler) addNotify(clientId int64) {
	channel := make(chan NotifyMsg, 1)
	sc.notifyChan[clientId] = channel
}

/*关闭id对应的chan，并且将该chan从sc中移除*/
func (sc *ShardCtrler) stopNotify(clientId int64) {
	if channel, ok := sc.notifyChan[clientId]; ok {
		close(channel)
		delete(sc.notifyChan, clientId)
	}
}

func (sc *ShardCtrler) waitResp(clientId int64, method string) NotifyMsg {
	wait_timer := time.NewTimer(WaitOpTimeOut)
	select {
	case resp := <-sc.notifyChan[clientId]:
		{
			return resp
		}
	case <-wait_timer.C:
		{
			return NotifyMsg{
				WrongLeader: false,
				Err:         Err(fmt.Sprintf("[ShardCtrler-%d] %s timeout", sc.me, method)),
			}
		}
	}
}

/*
ShardCtrler是一个分布式的分片控制器，所以Join、Leave、Move、Query等操作需要使用raft进行共识
*/
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lock()
	defer sc.unlock()
	// 幂等操作，过滤掉重复请求
	if sc.isRepeated(args.ClientId, args.ReqId) {
		reply.WrongLeader = false
		reply.Err = Err(fmt.Sprintf("[ShardCtrler-%d] get repeat request(%d) from [Cli-%d]",
			sc.me, args.ReqId, args.ClientId))
		return
	}
	_, _, is_leader := sc.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     args.Servers,
		Method:   Join,
	})
	if !is_leader {
		reply.WrongLeader = true
		reply.Err = Err(fmt.Sprintf("[ShardCtrler-%d] is not a leader\n", sc.me))
		return
	}
	sc.lastReq[args.ClientId] = args.ReqId
	sc.addNotify(args.ClientId)
	// 等待Raft集群共识前解锁，以提高系统的并发能力
	sc.unlock()
	resp_msg := sc.waitResp(args.ClientId, Join)
	reply.WrongLeader = resp_msg.WrongLeader
	reply.Err = resp_msg.Err
	sc.lock()
	// 由于分片客户端和服务器交流并不频繁，所以采用临时通道通信
	sc.stopNotify(args.ClientId)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.lock()
	defer sc.unlock()
	_, _, is_leader := sc.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     args.GIDs,
		Method:   Leave,
	})
	if !is_leader {
		reply.WrongLeader = true
		reply.Err = Err(fmt.Sprintf("[ShardCtrler-%d] is not a leader\n", sc.me))
		return
	}
	sc.addNotify(args.ClientId)
	// 等待Raft集群共识前解锁，以提高系统的并发能力
	sc.unlock()
	sc.waitResp(args.ClientId, Leave)
	sc.lock()
	// 由于分片客户端和服务器交流并不频繁，所以采用临时通道通信
	sc.stopNotify(args.ClientId)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.lock()
	defer sc.unlock()
	_, _, is_leader := sc.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     []int{args.Shard, args.GID},
		Method:   Move,
	})
	if !is_leader {
		reply.WrongLeader = true
		reply.Err = Err(fmt.Sprintf("[ShardCtrler-%d] is not a leader\n", sc.me))
		return
	}
	sc.addNotify(args.ClientId)
	// 等待Raft集群共识前解锁，以提高系统的并发能力
	sc.unlock()
	sc.waitResp(args.ClientId, Move)
	sc.lock()
	// 由于分片客户端和服务器交流并不频繁，所以采用临时通道通信
	sc.stopNotify(args.ClientId)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.handleQuery(args, reply)
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
			var notify NotifyMsg
			// 幂等
			if !isRepeated {
				sc.unlock()
				continue
			}
			// 处理不同种类的请求
			switch op.Method {
			case Join:
				sc.handleJoin(&JoinArgs{ClientId: op.ClientId, ReqId: op.ReqId, Servers: op.Args.(map[int][]string)})
			case Leave:
				sc.handleLeave(&LeaveArgs{ClientId: op.ClientId, ReqId: op.ReqId, GIDs: op.Args.([]int)})
			case Move:
				args := op.Args.([]int)
				sc.handleMove(&MoveArgs{ClientId: op.ClientId, ReqId: op.ReqId, Shard: args[0], GID: args[1]})
			}
			if ch, ok := sc.notifyChan[op.ClientId]; ok {
				DPrintf("[ShardCtrler-%d] send notify to [cli-%d], details: %v\n", sc.me, op.ClientId, notify)
				ch <- notify
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
