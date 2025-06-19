package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
	dead    int32
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
	// Config      Config
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
	sc.lock()
	ch := sc.notifyChan[clientId]
	sc.unlock()
	wait_timer := time.NewTimer(WaitOpTimeOut)
	select {
	case resp := <-ch:
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

func (sc *ShardCtrler) is_dead() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

/*
ShardCtrler是一个分布式的分片控制器，所以Join、Leave、Move、Query等操作需要使用raft进行共识
*/
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lock()
	defer sc.unlock()
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
	msg := sc.waitResp(args.ClientId, Leave)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
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
	msg := sc.waitResp(args.ClientId, Move)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
	sc.lock()
	// 由于分片客户端和服务器交流并不频繁，所以采用临时通道通信
	sc.stopNotify(args.ClientId)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.lock()
	defer sc.unlock()
	_, _, is_leader := sc.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     args.Num,
		Method:   Query,
	})
	if !is_leader {
		reply.WrongLeader = true
		reply.Err = Err(fmt.Sprintf("[ShardCtrler-%d] is not a leader\n", sc.me))
		return
	}
	sc.addNotify(args.ClientId)
	sc.unlock()
	// 这一步是保证Query时，Query之前的操作Join、Leave、Move操作都完成
	sc.waitResp(args.ClientId, Query)
	sc.lock()
	sc.handleQuery(args, reply)
	sc.stopNotify(args.ClientId)
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
	for !sc.is_dead() {
		select {
		case msg := <-sc.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			sc.lock()
			isRepeated := sc.isRepeated(op.ClientId, op.ReqId)
			// 幂等
			if isRepeated {
				DPrintf("[ShardCtrler-%d] 幂等操作触发\n", sc.me)
				sc.unlock()
				continue
			}
			sc.lastReq[op.ClientId] = op.ReqId
			// 处理不同种类的请求
			switch op.Method {
			case Join:
				sc.handleJoin(&JoinArgs{ClientId: op.ClientId, ReqId: op.ReqId, Servers: op.Args.(map[int][]string)})
			case Leave:
				sc.handleLeave(&LeaveArgs{ClientId: op.ClientId, ReqId: op.ReqId, GIDs: op.Args.([]int)})
			case Move:
				args := op.Args.([]int)
				sc.handleMove(&MoveArgs{ClientId: op.ClientId, ReqId: op.ReqId, Shard: args[0], GID: args[1]})
			case Query:
			}
			if ch, ok := sc.notifyChan[op.ClientId]; ok {
				notify := NotifyMsg{
					WrongLeader: false,
					Err:         OK,
				}
				DPrintf("[ShardCtrler-%d] send notify to [cli-%d], details: %v\n", sc.me, op.ClientId, notify)
				ch <- notify
			}
			sc.unlock()
		}
	}
}

func (sc *ShardCtrler) rebalance(conf *Config) {
	groups_size := len(conf.Groups)
	// 所有groups各持有什么shard
	hold := make(map[int][]int)
	gids := make([]int, groups_size)
	i := 0
	for gid := range conf.Groups {
		hold[gid] = []int{}
		gids[i] = gid
		i++
	}
	sort.Ints(gids)
	// 计算分片平均后，每个组平均应该有多少个分片，并且有多少个组需要比平均多一个分片
	averge, leave := NShards/groups_size, NShards%groups_size
	// 存放有多余shard的group在平衡后多余的shard，用于分配给shard不够的group
	realloc := []int{}
	for i := 0; i < NShards; i++ {
		if conf.Shards[i] == 0 {
			realloc = append(realloc, i)
		} else {
			hold[conf.Shards[i]] = append(hold[conf.Shards[i]], i)
		}
	}

	// need为各个groups还需要多少个shard才平衡
	need := make(map[int]int)
	target_shards := make(map[int][]int)
	// 对多余进行裁剪
	// 裁剪和分配分成两次for循环是为了尽可能减少分片移动
	// 并且分片最多的组和最少的组差距<=1
	for _, gid := range gids {
		shards := hold[gid]
		shards_count := len(shards)
		if shards_count > averge+1 && leave != 0 {
			// 有余数的时候，意味着结果中有些节点多，有些节点少
			// 所以将本身就多的节点保留average+1个
			realloc = append(realloc, shards[averge+1:]...)
			target_shards[gid] = shards[:averge+1]
			leave--
		} else if shards_count == averge+1 && leave != 0 {
			need[gid] = 0
			leave--
		} else if shards_count > averge && leave == 0 {
			realloc = append(realloc, shards[averge:]...)
			target_shards[gid] = shards[:averge]
		} else {
			// 不满一半的增加到avg需要的分片数
			need[gid] = averge - shards_count
		}
	}
	need_count := 0
	for _, count := range need {
		if count < 0 {
			fmt.Printf("[ShardCtrler-%d] has err, count: %d < 0\n", sc.me, count)
		}
		need_count += count
	}
	DPrintf("[ShardCtrler-%d] need_count=%d, realloc_count=%d\n", sc.me, need_count, len(realloc))
	// gids 和 realloc进行sort用于避免map遍历无序的问题
	// 对缺口进行分配
	for _, gid := range gids {
		need := need[gid]
		if leave != 0 && need >= 0 {
			// 多分配一个, 缩小余数
			target_shards[gid] = append(target_shards[gid], realloc[:need+1]...)
			realloc = realloc[need+1:]
			leave--
		} else if need != 0 {
			target_shards[gid] = append(target_shards[gid], realloc[:need]...)
			realloc = realloc[need:]
		}
	}
	// 不为0是错误，sleep用来排查
	// DPrintf("[ShardCtrler-%d] rebalance 完成\n", sc.me)
	if leave != 0 {
		fmt.Println("error, leave != 0")
		time.Sleep(30 * time.Second)
	}
	for gid, shards := range target_shards {
		for i := 0; i < len(shards); i++ {
			conf.Shards[shards[i]] = gid
		}
	}
}

func (sc *ShardCtrler) handleJoin(args *JoinArgs) {
	conf := sc.getConfig(-1)
	conf.Num++
	// conf := sc.getConfig(-1) 中map为浅拷贝，要深拷贝
	groups := make(map[int][]string, len(conf.Groups))
	for k, v := range conf.Groups {
		groups[k] = v
	}
	conf.Groups = groups
	// 深拷贝完成
	DPrintf("[ShardCtrler-%d] 通过Join操作生成 [config-%d]\n", sc.me, conf.Num)
	for k, v := range args.Servers {
		conf.Groups[k] = v
	}
	sc.rebalance(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleLeave(args *LeaveArgs) {
	conf := sc.getConfig(-1)
	conf.Num++
	// conf := sc.getConfig(-1) 中map为浅拷贝，要深拷贝
	groups := make(map[int][]string, len(conf.Groups))
	for k, v := range conf.Groups {
		groups[k] = v
	}
	conf.Groups = groups
	// 深拷贝完成
	DPrintf("[ShardCtrler-%d] 通过Leave操作生成 [config-%d]\n", sc.me, conf.Num)
	// 删除Leave的groups
	// 并找出Leave的groups分配的分片
	for i := 0; i < len(args.GIDs); i++ {
		delete(conf.Groups, args.GIDs[i])
		for j := 0; j < NShards; j++ {
			if conf.Shards[j] == args.GIDs[i] {
				conf.Shards[j] = 0
			}
		}
	}
	// Leave的两种情况：1. Leave之后还有剩余的组；2. Leave之后剩下0组
	if len(conf.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			conf.Shards[i] = 0
		}
	} else {
		// 将这些为被分配的分片重新分配给其他组
		sc.rebalance(&conf)
	}
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleMove(args *MoveArgs) {
	conf := sc.getConfig(-1)
	// 由于Move不会修改map，故不需要深拷贝
	conf.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, conf)
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
	atomic.StoreInt32(&sc.dead, 1)
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
	sc.lastReq = make(map[int64]ReqId)
	sc.notifyChan = make(map[int64]chan NotifyMsg)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.handleApplyCmd()
	return sc
}
