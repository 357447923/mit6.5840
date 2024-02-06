package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
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
	ReqId    int64
	Key      string
	Value    string
	Method   string
}

type NotifyMsg struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	msgNotify map[int64]chan NotifyMsg
	lastCmd   map[int64]int64
	data      map[string]string
}

func (kv *KVServer) isRepeat(clientId int64, reqId int64) bool {
	if last, ok := kv.lastCmd[clientId]; ok {
		return last == reqId
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

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	var data map[string]string
	err := d.Decode(&data)
	if err != nil {
		panic(err)
	}
	var lastCmd map[int64]int64
	err = d.Decode(&lastCmd)
	if err != nil {
		panic(err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = data
	kv.lastCmd = lastCmd
}

func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				// 加载Snapshot
				// loadSnapshot内部有锁
				kv.loadSnapshot(msg.Snapshot)
				DPrintf("follower=%d load snapshot success\n", kv.me)
				continue
			}
			op := msg.Command.(Op)
			value := ""
			var err Err
			err = ""
			switch op.Method {
			case PutOp:
				if !kv.isRepeat(op.ClientId, op.ReqId) {
					kv.data[op.Key] = op.Value
					DPrintf("server=%d put {\"%v\" : \"%v\"}\n", kv.me, op.Key, op.Value)
				}
				value = op.Value
				err = OK
			case AppendOp:
				err, value = kv.getData(op.Key)
				if !kv.isRepeat(op.ClientId, op.ReqId) {
					if err == OK {
						value += op.Value
						kv.data[op.Key] = value
					} else if err == ErrNoKey {
						kv.data[op.Key] = op.Value
						err = OK
					}
					//DPrintf("server=%d refresh {\"%v\" : \"%v\"}\n", kv.me, op.Key, kv.data[op.Key])
				}
			case GetOp:
				err, value = kv.getData(op.Key)
			default:
				panic(fmt.Sprintf("unknown method=%v\n", op.Method))
			}
			kv.lastCmd[op.ClientId] = op.ReqId
			//DPrintf("server=%d apply op=%v\n", kv.me, op)
			kv.mu.Lock()
			if ch, ok := kv.msgNotify[op.ReqId]; ok {
				if len(ch) == 0 {
					ch <- NotifyMsg{
						Value: value,
						Err:   err,
					}
				} else {
					timeout := time.NewTimer(WaitOpTimeOut)
					select {
					case <-timeout.C:
						DPrintf("follower=%d say I'm timeout\n", kv.me)
					case ch <- NotifyMsg{
						Value: value,
						Err:   err,
					}:
					}
				}
			}
			kv.mu.Unlock()
			// 都是单个线程的操作，所以saveSnapshot应该是不需要上锁的
			kv.saveSnapshot(msg.CommandIndex)
		}
	}
}

func (kv *KVServer) waitCmd(op Op) (resp NotifyMsg) {
	kv.rf.Start(op)
	DPrintf("server=%d已经发送请求\n", kv.me)
	kv.mu.Lock()
	DPrintf("server=%d锁获取成功\n", kv.me)
	if _, ok := kv.msgNotify[op.ReqId]; ok {
		kv.mu.Unlock()
		resp.Err = ErrTimeOut
		return
	}
	kv.msgNotify[op.ReqId] = make(chan NotifyMsg, 1)
	kv.mu.Unlock()
	DPrintf("server=%d解锁成功\n", kv.me)
	stop := time.NewTimer(WaitOpTimeOut)
	defer stop.Stop()
	select {
	case <-stop.C:
		resp.Err = ErrTimeOut
	case resp = <-kv.msgNotify[op.ReqId]:
		//DPrintf("server=%d handle success. op=%v, resp.Err=%v\n", kv.me, op, resp.Err)
	}
	DPrintf("server=%d say hello, resp.Err=%v\n", kv.me, resp.Err)
	kv.mu.Lock()
	delete(kv.msgNotify, op.ReqId)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		_, isLeader = kv.rf.GetState()
		DPrintf("server=%d return wrong leader. isLeader=%v\n", kv.me, isLeader)
		return
	}
	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
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
		_, isLeader = kv.rf.GetState()
		DPrintf("server=%d return wrong leader. isLeader=%v\n", kv.me, isLeader)
		return
	}
	DPrintf("I=%d come to here\n", kv.me)
	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
	}
	resMsg := kv.waitCmd(op)
	reply.Err = resMsg.Err
	DPrintf("server=%d execute put append success. op=%v, resp=%v. cost=%vms\n",
		kv.me, op, reply, time.Now().UnixMilli()-start)
}

func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.maxraftstate >= kv.persister.RaftStateSize() {
		return
	}
	size := kv.persister.RaftStateSize()
	start := time.Now().UnixMilli()
	DPrintf("server=%d saving snapshot\n", kv.me)
	data := kv.genSnapshotData()
	kv.rf.Snapshot(logIndex, data)
	DPrintf("server=%d save snapshot success. o'size=%d c'size=%d cost=%v\n", kv.me, size, kv.persister.RaftStateSize(), time.Now().UnixMilli()-start)
	return
}

func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.data)
	if err != nil {
		panic(err)
	}
	err = e.Encode(kv.lastCmd)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
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
	var err Err
	err = ""
	labgob.Register(err)
	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.msgNotify = make(map[int64]chan NotifyMsg)
	kv.lastCmd = make(map[int64]int64)
	// You may need initialization code here.
	go kv.handleApplyCh()

	return kv
}
