package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const WaitOpTimeOut = 500 * time.Millisecond
const RefreshLap = 100 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	ReqId    ReqId
	Args     interface{}
	Method   string
}

type NotifyMsg struct {
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastReq    map[int64]ReqId
	notifyChan map[int64]chan NotifyMsg
	data       map[string]string
	clerk      *shardctrler.Clerk
	config     shardctrler.Config // 或许可以定时请求配置加客户端发送了个请求后发现不是本节点时请求最新config
}

func (kv *ShardKV) lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) is_repeated(clientId int64, reqId ReqId) bool {
	if val, ok := kv.lastReq[clientId]; ok {
		return val == reqId
	}
	return false
}

func (kv *ShardKV) addNotifyNotExist(clientId int64) {
	if _, ok := kv.notifyChan[clientId]; !ok {
		kv.notifyChan[clientId] = make(chan NotifyMsg)
	}
}

func (kv *ShardKV) stop_notify(clientId int64) {
	if ch, ok := kv.notifyChan[clientId]; ok {
		close(ch)
		delete(kv.notifyChan, clientId)
	}

}

func (kv *ShardKV) is_killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) waitResp(clientId int64) (msg NotifyMsg) {
	timeOut := time.NewTimer(WaitOpTimeOut)
	select {
	case msg = <-kv.notifyChan[clientId]:
	case <-timeOut.C:
		msg.Err = ErrTimeout
	}
	return
}

func (kv *ShardKV) isExchange(clientId int64) bool {
	_, ok := kv.config.Groups[int(clientId)]
	return ok
}

func (kv *ShardKV) keyBelongGroups(key string) bool {
	shard := key2shard(key)
	return kv.gid == kv.config.Shards[shard]
}

func (kv *ShardKV) handleGet(key string, reply *GetReply) {
	if val, ok := kv.data[key]; ok {
		reply.Err = OK
		reply.Value = val
		return
	}
	reply.Err = ErrNoKey
}

func (kv *ShardKV) handlePut(key string, value string) {
	kv.data[key] = value
}

func (kv *ShardKV) handleAppend(key string, value string) {
	kv.data[key] += value
}

func (kv *ShardKV) handleApply() {
	for !kv.is_killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			var notify_msg NotifyMsg
			kv.lock()
			if kv.is_repeated(op.ClientId, op.ReqId) {
				kv.unlock()
				fmt.Printf("[ShardKV-%d] 去重\n", kv.me)
				continue
			}
			kv.lastReq[op.ClientId] = op.ReqId
			fmt.Printf("[ShardKV-%d] 正在处理 %v\n", kv.me, msg)
			switch op.Method {
			case Get:
				key := op.Args.(string)
				if !kv.isExchange(op.ClientId) && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
				}
				notify_msg.Err = OK
			case Put:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if !kv.isExchange(op.ClientId) && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				kv.handlePut(key, value)
				notify_msg.Err = OK
			case Append:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if !kv.isExchange(op.ClientId) && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				kv.handleAppend(key, value)
				notify_msg.Err = OK
			}
			if ch, ok := kv.notifyChan[op.ClientId]; ok {
				ch <- notify_msg
				fmt.Printf("[ShardKV-%d] 获取到了notify_chan\n", kv.me)
			}
			kv.unlock()
		}
	}
}

func (kv *ShardKV) refreshConf() {
	conf := kv.clerk.Query(-1)
	kv.lock()
	defer kv.unlock()
	if kv.config.Num == conf.Num {
		return
	}
	_, is_leader := kv.rf.GetState()
	if !is_leader {
		kv.config = conf
		return
	}
	// 让Leader做分片迁移
	// 分片迁移
	/*myNewShards := []int{}
	myOldShards := []int{}
	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			myOldShards = append(myOldShards, shard)
		}
	}
	for shard, gid := range conf.Shards {
		if gid == kv.gid {
			myNewShards = append(myNewShards, shard)
		}
	}

	kv.unlock()*/
}

func (kv *ShardKV) timedRefresh() {
	ticker := time.NewTicker(RefreshLap)
	for !kv.is_killed() {
		select {
		case <-ticker.C:
			kv.refreshConf()
		}
	}
	ticker.Stop()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	_, _, is_leader := kv.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     args.Key,
		Method:   Get,
	})
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.addNotifyNotExist(args.ClientId)
	kv.unlock()
	notify := kv.waitResp(args.ClientId)
	reply.Err = notify.Err
	if notify.Err != OK {
		kv.lock()
		return
	}
	kv.lock()
	kv.handleGet(args.Key, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	_, _, is_leader := kv.rf.Start(Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Args:     []string{args.Key, args.Value},
		Method:   args.Op,
	})
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.addNotifyNotExist(args.ClientId)
	kv.unlock()
	reply.Err = kv.waitResp(args.ClientId).Err
	if reply.Err == OK {
		fmt.Printf("[ShardKV-%d] 返回了OK\n", kv.me)
	}
	kv.lock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastReq = make(map[int64]ReqId)
	kv.notifyChan = make(map[int64]chan NotifyMsg)
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.data = make(map[string]string)
	for kv.config.Num == 0 {
		time.Sleep(100 * time.Millisecond)
		kv.config = kv.clerk.Query(-1)
	}

	go kv.timedRefresh()
	go kv.handleApply()

	return kv
}
