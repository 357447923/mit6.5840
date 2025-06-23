package shardkv

import (
	"bytes"
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
	persister    *raft.Persister

	// Your definitions here.
	lastReq          map[int64]ReqId
	notifyChan       map[int64]chan NotifyMsg
	data             map[string]string
	clerk            *shardctrler.Clerk
	config           shardctrler.Config
	confAble         int32 // 配置是否可用
	applyAble        int32 // 日志重放是否完成
	confChangeNorify chan int32
}

func (kv *ShardKV) lock() {
	// fmt.Printf("[ShardKV-%d] lock\n", kv.me)
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	// fmt.Printf("[ShardKV-%d] unlock\n", kv.me)
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
		kv.notifyChan[clientId] = make(chan NotifyMsg, 10)
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
	// fmt.Printf("[ShardKV-%d] lock\n", kv.me)
	kv.lock()
	ch := kv.notifyChan[clientId]
	// fmt.Printf("[ShardKV-%d] unlock\n", kv.me)
	kv.unlock()
	timeOut := time.NewTimer(WaitOpTimeOut)
	select {
	case msg = <-ch:
		// fmt.Printf("[ShardKV-%d] 收到了notify\n", kv.me)
	case <-timeOut.C:
		msg.Err = ErrTimeout
		kv.stop_notify(clientId)
		// fmt.Printf("[ShardKV-%d] 超时\n", kv.me)
	}
	return
}

func (kv *ShardKV) isExchange(clientId int64) bool {
	for gid := range kv.config.Groups {
		// TODO 应该不能这样做
		if clientId == int64(gid) {
			return true
		}
	}
	return false
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
	Dprintf("[ShardKV-%d-%d] before put [%s:%s], cur data=%v\n", kv.gid, kv.me, key, value, kv.data)
	kv.data[key] = value
	Dprintf("[ShardKV-%d-%d] put [%s:%s] cur data=%v\n", kv.gid, kv.me, key, value, kv.data)
}

func (kv *ShardKV) handleAppend(key string, value string) {
	kv.data[key] += value
}

func (kv *ShardKV) handleApply() {
	if kv.rf.GetCommitIdx() == 0 {
		kv.lock()
		kv.applyAble = 1
		kv.unlock()
	}
	for !kv.is_killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				if !msg.SnapshotValid {
					continue
				}
				// 处理Snapshot
				kv.loadSnapshot(msg.Snapshot)
				commitIdx := kv.rf.GetCommitIdx()
				if msg.SnapshotIndex == commitIdx {
					Dprintf("[ShardKV-%d-%d] 重放完成, data: %v\n", kv.gid, kv.me, kv.data)
					kv.applyAble = 1
				}
				continue
			}
			// 处理Command
			op := msg.Command.(Op)
			var notify_msg NotifyMsg

			if kv.is_repeated(op.ClientId, op.ReqId) {
				Dprintf("[ShardKV-%d-%d] 去重\n", kv.gid, kv.me)
				continue
			}
			kv.lastReq[op.ClientId] = op.ReqId
			switch op.Method {
			case Get:
				key := op.Args.(string)
				if kv.applyAble == 1 && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				fmt.Printf("[ShardKV-%d-%d], 正在处理 %v, data:%v\n", kv.gid, kv.me, op, kv.data)
				notify_msg.Err = OK
			case Put:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if kv.applyAble == 1 && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				kv.handlePut(key, value)
				notify_msg.Err = OK
			case Append:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if kv.applyAble == 1 && !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				// fmt.Printf("[ShardKV-%d-%d] 正在处理 %v\n", kv.gid, kv.me, op)
				kv.handleAppend(key, value)
				notify_msg.Err = OK
			case Transform:
				data := op.Args.(map[string]string)
				for k, v := range data {
					kv.handlePut(k, v)
				}
				notify_msg.Err = OK
			}
			kv.lock()
			if ch, ok := kv.notifyChan[op.ClientId]; ok {
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					kv.stop_notify(op.ClientId)
				} else {
					ch <- notify_msg
				}
			}
			// 确定重放完成后启动服务
			if kv.applyAble == 0 {
				Dprintf("[ShardKV-%d-%d] 重放 %v\n", kv.gid, kv.me, op)
				commitIdx := kv.rf.GetCommitIdx()
				if msg.CommandIndex == commitIdx {
					Dprintf("[ShardKV-%d-%d] 重放完成, data: %v\n", kv.gid, kv.me, kv.data)
					kv.applyAble = 1
				}
			}
			kv.unlock()
			fmt.Printf("[ShardKV-%d-%d] 处理 %v 完成\n", kv.gid, kv.me, op)
			kv.saveSnapshot(msg.CommandIndex)
		}
	}
}

// 把shard对应的数据发送给server，这个函数可以异步进行
func (kv *ShardKV) sendPut(servers []string, shard int, notifyChan chan int32) {
	putData := make(map[string]string)
	kv.lock()
	for key, val := range kv.data {
		if shard == key2shard(key) {
			putData[key] = val
		}
	}
	kv.unlock()

	args := ServerPutArgs{
		From:  kv.gid,
		ReqId: ReqId(nrand()),
		Data:  putData,
	}
	var reply GetReply
	retry := 0
	for i := 0; i < len(servers); i++ {
		srv := kv.make_end(servers[i])
		ok := srv.Call("ShardKV.ServerTransform", &args, &reply)
		if ok && reply.Err == OK {
			notifyChan <- 1 // 通知主线程该goroughting完成
			return
		}
		if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNoAble) {
			// 接收到wrongGroup有两种可能
			// 1. Conf又更新了
			// 2. 或者是对方的Conf还未更新
			// 解决方法：尝试多次等待后重发，若多次失败则断定是Conf又更新了，则通知主线程
			if retry == 5 {
				notifyChan <- 0
				return
			}
			time.Sleep(100 * time.Millisecond)
			i--
			retry++
		}
		// ... not ok, or ErrWrongLeader继续重试
	}
	// 作为保险
	notifyChan <- 0

}

func (kv *ShardKV) refreshConf() {
	conf := kv.clerk.Query(-1)
	if conf.Num == 0 || kv.config.Num == conf.Num {
		return
	}
	kv.lock()
	defer kv.unlock()
	kv.confAble = 0
	_, is_leader := kv.rf.GetState()
	if !is_leader {
		kv.config = conf
		kv.confAble = 1
		return
	}
	// 发送丢失的分片数据
	oldShards := kv.config.Shards
	// 拷贝一份原来的Shard用于做分片转移，然后修改配置
	// 这样可以做到没有变化的分片和新分配给该节点的分片仍能处理数据
	// 提高系统整体的吞吐量
	kv.config = conf
	kv.confAble = 1
	kv.unlock()

	success := make(chan int32, shardctrler.NShards)
	needSend := 0
	for i := 0; i < shardctrler.NShards; i++ {
		diff := conf.Shards[i] != oldShards[i]
		if diff && oldShards[i] == kv.gid {
			servers := conf.Groups[conf.Shards[i]]
			needSend++
			// 分片数据转移
			go kv.sendPut(servers, i, success)
		}
	}
	// 等待分片数据发送完成
	for needSend > 0 {
		select {
		case <-success:
			needSend--
		}
	}

	fmt.Printf("[ShardKV-%d-%d] 接收到 [Conf-%d]: shard:%v\n", kv.gid, kv.me, conf.Num, conf.Shards)
	kv.lock()
}

// 使用该操作前必须上锁
func (kv *ShardKV) isAble() bool {
	return kv.confAble == 1 && kv.applyAble == 1
}

func (kv *ShardKV) refreshTask() {
	// 快速读取配置
	if kv.confAble == 0 {
		kv.refreshConf()
	}
	ticker := time.NewTicker(RefreshLap)
	for !kv.is_killed() {
		select {
		case <-ticker.C:
			kv.refreshConf()
		}
	}
	ticker.Stop()
}
func (kv *ShardKV) saveSnapshot(logIdx int) {
	if kv.maxraftstate == -1 || kv.maxraftstate >= kv.persister.RaftStateSize() {
		return
	}
	size := kv.persister.RaftStateSize()
	start := time.Now().UnixMilli()
	data := kv.genSnapshotData()
	kv.rf.Snapshot(logIdx, data)
	Dprintf("[ShardKV-%d-%d] save snapshot success. o'size=%d c'size=%d cost=%v\n", kv.gid, kv.me, size, kv.persister.RaftStateSize(), time.Now().UnixMilli()-start)
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.data)
	if err != nil {
		panic(err)
	}
	err = e.Encode(kv.lastReq)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *ShardKV) loadSnapshot(snapshot []byte) {
	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	var data map[string]string
	err := d.Decode(&data)
	if err != nil {
		panic(err)
	}
	var lastReq map[int64]ReqId
	err = d.Decode(&lastReq)
	if err != nil {
		panic(err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = data
	kv.lastReq = lastReq
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	if !kv.isAble() {
		reply.Err = ErrNoAble
		return
	}
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
	if !kv.isAble() {
		reply.Err = ErrNoAble
		return
	}
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
		Dprintf("[ShardKV-%d] 返回了OK\n", kv.me)
	}
	kv.lock()
}

func (kv *ShardKV) ServerTransform(args *ServerPutArgs, reply *ServerPutReply) {
	kv.lock()
	defer kv.unlock()
	if !kv.isAble() {
		reply.Err = ErrNoAble
		return
	}
	_, _, is_leader := kv.rf.Start(Op{
		ClientId: int64(args.From),
		ReqId:    args.ReqId,
		Args:     args.Data,
		Method:   Transform,
	})
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.addNotifyNotExist(int64(args.From))
	kv.unlock()
	reply.Err = kv.waitResp(int64(args.From)).Err
	kv.lock()
	kv.stop_notify(int64(args.From))
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
	labgob.Register(map[string]string{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
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
	kv.confAble = 0
	kv.applyAble = 0
	kv.confChangeNorify = make(chan int32)

	fmt.Printf("[ShardKV-%d-%d] 启动, 需要重放 %d 条日志\n", kv.gid, kv.me, kv.rf.GetCommitIdx())

	go kv.handleApply()
	go kv.refreshTask()

	return kv
}
