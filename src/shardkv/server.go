package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const WaitOpTimeOut = 500 * time.Millisecond

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
	WrongLeader bool
	Err         string
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

func (kv *ShardKV) is_killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) handleApply() {
	for !kv.is_killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			kv.lock()
			if kv.is_repeated(op.ClientId, op.ReqId) {
				kv.unlock()
				continue
			}
			switch op.Method {
			case Get:

			case Put:

			}
			notify_msg := NotifyMsg{
				WrongLeader: false,
				Err:         OK,
			}
			if ch, ok := kv.notifyChan[op.ClientId]; ok {
				ch <- notify_msg
			}
			kv.unlock()
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	kv.rf.Start(Op{})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
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
	go kv.handleApply()

	return kv
}
