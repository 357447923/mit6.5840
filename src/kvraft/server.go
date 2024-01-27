package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"
)

const WaitOpTimeOut = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Key      string
	Value    string
	Method   string
}

type NotifyMsg struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	msgNotify map[Op]chan NotifyMsg
	lastCmd   map[int64]Op
	data      map[string]string
}

func (kv *KVServer) isRepeat(clientId int64, op Op) bool {
	if last, ok := kv.lastCmd[clientId]; ok {
		return last == op
	}
	return false
}

func (kv *KVServer) getData(key string) (err Err, val string) {
	if v, ok := kv.data[key]; ok {
		err = OK
		val = v
	} else {
		err = ErrNoKey
	}
	return
}

func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			var value string
			var err Err
			switch op.Method {
			case PutOp:
				if !kv.isRepeat(op.ClientId, op) {
					kv.data[op.Key] = op.Value
					DPrintf("server=%d put {\"%v\" : \"%v\"}\n", kv.me, op.Key, op.Value)
				}
				value = op.Value
				err = OK
			case AppendOp:
				err, value = kv.getData(op.Key)
				if !kv.isRepeat(op.ClientId, op) {
					if err == OK {
						value += op.Value
						kv.data[op.Key] = value
					} else if err == ErrNoKey {
						kv.data[op.Key] = op.Value
						err = OK
					}
					DPrintf("server=%d refresh {\"%v\" : \"%v\"}\n", kv.me, op.Key, kv.data[op.Key])
				}
			case GetOp:
				err, value = kv.getData(op.Key)
			default:
				panic(fmt.Sprintf("unknown method=%v\n", op.Method))
			}
			kv.lastCmd[op.ClientId] = op
			DPrintf("server=%d apply op=%v\n", kv.me, op)
			kv.mu.Lock()
			if ch, ok := kv.msgNotify[op]; ok {
				ch <- NotifyMsg{
					Value: value,
					Err:   err,
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) waitCmd(op Op) (res NotifyMsg) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.msgNotify[op] = make(chan NotifyMsg)
	kv.mu.Unlock()
	stop := time.NewTimer(WaitOpTimeOut)
	defer stop.Stop()
	select {
	case <-stop.C:
		res.Err = ErrTimeOut
	case msg := <-kv.msgNotify[op]:
		res = msg
	}
	kv.mu.Lock()
	delete(kv.msgNotify, op)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Key:      args.Key,
		Method:   GetOp,
	}
	resMsg := kv.waitCmd(op)
	reply.Value, reply.Err = resMsg.Value, resMsg.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	start := time.Now().UnixMilli()
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ClientId: args.ClientId,
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
	}
	resMsg := kv.waitCmd(op)
	reply.Err = resMsg.Err
	DPrintf("server=%d execute put append success. cost=%vms\n", kv.me, time.Now().UnixMilli()-start)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.msgNotify = make(map[Op]chan NotifyMsg)
	kv.lastCmd = make(map[int64]Op)
	// You may need initialization code here.
	go kv.handleApplyCh()

	return kv
}
