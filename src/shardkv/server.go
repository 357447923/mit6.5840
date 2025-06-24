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

type TransformNotify struct {
	Err   Err
	Shard int
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
	lastReq    map[int64]ReqId
	notifyChan map[int64]chan NotifyMsg
	data       map[string]string
	clerk      *shardctrler.Clerk
	config     shardctrler.Config
	putAble    int32   // 是否可以执行PUT操作，TODO 可以将该变量使用改为原子变量操作
	getAble    []int32 // 是否可以执行GET操作，TODO 可以将该变量使用改为原子变量操作
	applyAble  int32   // 日志重放是否完成
	confAble   int32   // 当前配置是否已经更新到已知的最新配置
	ready      int32   // 准备更新日志
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
	kv.lock()
	ch := kv.notifyChan[clientId]
	kv.unlock()
	timeOut := time.NewTimer(WaitOpTimeOut)
	select {
	case msg = <-ch:
	case <-timeOut.C:
		msg.Err = ErrTimeout
		kv.stop_notify(clientId)
	}
	return
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
	// Dprintf("[ShardKV-%d-%d] put [%s:%s] cur data=%v\n", kv.gid, kv.me, key, value, kv.data)
}

func (kv *ShardKV) handleAppend(key string, value string) {
	kv.data[key] += value
}

func (kv *ShardKV) handleApply() {
	if kv.rf.GetCommitIdx() == 0 {
		atomic.StoreInt32(&kv.applyAble, 1)
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
					atomic.StoreInt32(&kv.applyAble, 1)
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
				if !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				// fmt.Printf("[ShardKV-%d-%d], 正在处理 %v, data:%v\n", kv.gid, kv.me, op, kv.data)
				notify_msg.Err = OK
			case Put:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				kv.handlePut(key, value)
				notify_msg.Err = OK
			case Append:
				key, value := op.Args.([]string)[0], op.Args.([]string)[1]
				if !kv.keyBelongGroups(key) {
					notify_msg.Err = ErrWrongGroup
					break
				}
				// fmt.Printf("[ShardKV-%d-%d] 正在处理 %v\n", kv.gid, kv.me, op)
				kv.handleAppend(key, value)
				notify_msg.Err = OK
			case Transform:
				data := op.Args.(TransformData)
				if kv.gid != kv.config.Shards[data.Shard] {
					notify_msg.Err = ErrWrongGroup
					break
				}
				for k, v := range data.Data {
					kv.handlePut(k, v)
				}
				notify_msg.Err = OK
				kv.lock()
				kv.getAble[data.Shard] = 1
				kv.unlock()
			case PrepareUpdate:
				// 准备日志的更新
				newShards := op.Args.(shardctrler.Config).Shards
				kv.lock()
				kv.prepareFresh()
				// 先让不变的恢复请求响应
				for i := 0; i < len(newShards); i++ {
					same := kv.config.Shards[i] == newShards[i]
					if (same && newShards[i] == kv.gid) || (kv.config.Shards[i] == 0 && newShards[i] == kv.gid) {
						kv.getAble[i] = 1
					}
				}
				Dprintf("[ShardKV-%d-%d] 准备更新配置 Conf-%d, getAble=%v\n", kv.gid, kv.me, op.Args.(shardctrler.Config).Num, kv.getAble)
				kv.unlock()
				notify_msg.Err = OK
			case Update:
				conf := op.Args.(shardctrler.Config)
				kv.handleUpdate(conf)
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
			kv.unlock()
			// 确定重放完成后启动服务
			if atomic.LoadInt32(&kv.applyAble) == 0 {
				Dprintf("[ShardKV-%d-%d] 重放 %v\n", kv.gid, kv.me, op)
				commitIdx := kv.rf.GetCommitIdx()
				if msg.CommandIndex == commitIdx {
					Dprintf("[ShardKV-%d-%d] 重放完成, data: %v\n", kv.gid, kv.me, kv.data)
					atomic.StoreInt32(&kv.applyAble, 1)
				}
			}
			// fmt.Printf("[ShardKV-%d-%d] 处理 %v 完成\n", kv.gid, kv.me, op)
			kv.saveSnapshot(msg.CommandIndex)
		}
	}
}

// 把shard对应的数据发送给server，这个函数可以异步进行
func (kv *ShardKV) transformShard(servers []string, shard int, notifyChan chan TransformNotify) {
	putData := make(map[string]string)
	kv.lock()
	for key, val := range kv.data {
		if shard == key2shard(key) {
			putData[key] = val
		}
	}

	args := ServerPutArgs{
		From:      kv.gid,
		ReqId:     ReqId(nrand()),
		Shard:     shard,
		ConfigNum: kv.config.Num,
		Data:      putData,
	}
	kv.unlock()
	var reply GetReply
	retry := 0
	for i := 0; i < len(servers); i++ {
		srv := kv.make_end(servers[i])
		reply.Err = ""
		ok := srv.Call("ShardKV.ServerTransform", &args, &reply)
		if ok && reply.Err == OK {
			notifyChan <- TransformNotify{Err: reply.Err, Shard: shard} // 通知主线程该goroughting完成
			// fmt.Printf("[ShardKV-%d-%d] 发送 Shard-%d 分片数据到 [ShardKV-%d-%d] 成功\n",
			// 	kv.gid, kv.me, shard, kv.config.Shards[shard], i)
			return
		}
		if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrNoAble) {
			// 接收到wrongGroup有两种可能
			// 1. Conf又更新了
			// 2. 或者是对方的Conf还未更新
			// 解决方法：尝试多次等待后重发，若多次失败则断定是Conf又更新了，则通知主线程
			if retry == 5 {
				notifyChan <- TransformNotify{Err: reply.Err, Shard: shard}
				// fmt.Printf("[ShardKV-%d-%d] 发送 Shard-%d 分片数据到 %s 失败, 错误信息:%s\n",
				// kv.gid, kv.me, shard, servers[i], reply.Err)
				return
			}
			time.Sleep(100 * time.Millisecond)
			i--
			retry++
			continue
		}
		// ... not ok, or ErrWrongLeader继续重试
		// fmt.Printf("[ShardKV-%d-%d] 发送 Shard-%d 分片数据到 %s 失败, 错误信息: %s\n",
		// kv.gid, kv.me, shard, servers[i], reply.Err)
	}
	// 作为保险
	notifyChan <- TransformNotify{Err: reply.Err, Shard: shard}
	Dprintf("[ShardKV-%d-%d] 发送 Shard-%d 分片数据到 %v 失败, 错误信息: %s\n",
		kv.gid, kv.me, shard, servers, reply.Err)
}

// 使用前上锁
func (kv *ShardKV) prepareFresh() {
	kv.putAble = 0
	kv.confAble = 0
	for i := 0; i < len(kv.config.Shards); i++ {
		kv.getAble[i] = 0
	}
	kv.ready = 1
}

func (kv *ShardKV) handleUpdate(conf shardctrler.Config) {
	fmt.Printf("[ShardKV-%d-%d] 正在更新 Conf-%d %v\n", kv.gid, kv.me, conf.Num, conf.Shards)
	kv.config = conf
	kv.lock()
	kv.putAble = 1
	kv.ready = 0
	kv.unlock()
	fmt.Printf("[ShardKV-%d-%d] 更新到 Conf-%d, getAble=%v\n", kv.gid, kv.me, conf.Num, kv.getAble)
}

func (kv *ShardKV) pushShardData(conf shardctrler.Config, oriConf shardctrler.Config) {
	notifies := make(chan TransformNotify, 1)
	// needSend := 0
	for i := 0; i < len(oriConf.Shards); i++ {
		same := conf.Shards[i] == oriConf.Shards[i]
		if (same && oriConf.Shards[i] == kv.gid) || oriConf.Shards[i] == 0 && conf.Shards[i] == kv.gid {
			kv.getAble[i] = 1
		} else if !same && oriConf.Shards[i] == kv.gid {
			servers := conf.Groups[conf.Shards[i]]
			// needSend++
			// go func(servers []string, i int, notifies chan TransformNotify) {
			// 	fmt.Printf("[ShardKV-%d-%d] 正在发送 Shard-%d 数据到 %v\n", kv.gid, kv.me, i, servers)
			// 	kv.transformShard(servers, i, notifies)
			// }(servers, i, notifies)
			for { // 串行发送, 可以再试试看改成并行，但是不知道为什么并行not ok的数量会显著变多
				fmt.Printf("[ShardKV-%d-%d] 正在发送分片Shard-%d 给 %v\n", kv.gid, kv.me, i, servers)
				kv.transformShard(servers, i, notifies)
				notify := <-notifies
				if notify.Err == OK {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	close(notifies)
	// kv.unlock()
	// // 先不要限制retry次数
	// retry := make([]int, len(conf.Shards))
	// for needSend > 0 {
	// 	select {
	// 	case notify := <-notifies:
	// 		if notify.Err == OK {
	// 			needSend--
	// 			continue
	// 		}
	// 		//失败重试
	// 		retry[notify.Shard]++
	// 		go func() {
	// 			time.Sleep(time.Millisecond * 200)
	// 			fmt.Printf("[ShardKV-%d-%d] 发送Shard-%d数据到 %v 失败, 正在重试。错误信息为: %s\n", kv.gid, kv.me, notify.Shard, conf.Groups[conf.Shards[notify.Shard]], notify.Err)
	// 			kv.transformShard(conf.Groups[conf.Shards[notify.Shard]], notify.Shard, notifies)
	// 		}()
	// 	}
	// }
	// close(notifies)
}

func (kv *ShardKV) refreshConf() {
	clientId := int64(kv.gid)
	if atomic.LoadInt32(&kv.applyAble) == 0 {
		return
	}
	kv.lock()
	defer kv.unlock()
	// 逐配置更新
	confNum := kv.config.Num + 1
	kv.unlock()
	conf := kv.clerk.Query(confNum)
	kv.lock()
	// 已经是最新了
	if conf.Num == 0 || conf.Num == kv.config.Num {
		if kv.confAble == 0 {
			for i := 0; i < len(conf.Shards); i++ {
				if conf.Shards[i] == kv.gid && kv.getAble[i] == 0 {
					return
				}
			}
		}
		kv.confAble = 1
		return
	}
	// Leader必须上一个Conf更新完成，才可以发送分片数据
	// ready等于1的时候，说明上一个Leader已经发现上一个配置已经全部完成
	// 并且在通知其他节点更新前宕机了
	if kv.ready == 0 {
		for i := 0; i < len(kv.config.Shards); i++ {
			if kv.config.Shards[i] == kv.gid && kv.getAble[i] == 0 {
				fmt.Printf("[ShardKV-%d-%d] 上一个Conf-%d: %v 未完成, getAble: %v\n", kv.gid, kv.me, kv.config.Num, kv.config.Shards, kv.getAble)
				return
			}
		}
	}

	_, _, isLeader := kv.rf.Start(Op{
		ClientId: clientId,
		ReqId:    ReqId(nrand()),
		Args:     conf,
		Method:   PrepareUpdate,
	})
	if !isLeader {
		return
	}
	kv.addNotifyNotExist(clientId)
	kv.unlock()
	msg := kv.waitResp(clientId)
	if msg.Err != OK {
		kv.lock()
		kv.stop_notify(clientId)
		fmt.Printf("ERROR: [ShardKV-%d-%d] 准备更新日志 失败, 信息：%s\n", kv.gid, kv.me, msg.Err)
		return
	}
	kv.pushShardData(conf, kv.config)
	Dprintf("[ShardKV-%d-%d] 发送分片数据完成\n", kv.gid, kv.me)
	kv.lock()
	// Leader共识给其他节点更新配置
	_, _, isLeader = kv.rf.Start(Op{
		ClientId: clientId,
		ReqId:    ReqId(nrand()),
		Args:     conf,
		Method:   Update,
	})
	// 说明发生了Leader切换
	if !isLeader {
		return
	}
	kv.addNotifyNotExist(clientId)
	kv.unlock()
	msg = kv.waitResp(clientId)
	if msg.Err == OK {
		fmt.Printf("[ShardKV-%d-%d] 更新 [Conf-%d] 完成\n", kv.gid, kv.me, conf.Num)
	}
	kv.lock()
	kv.stop_notify(clientId)
}

func (kv *ShardKV) refreshTask() {
	// 启动刷新配置任务时，先快速读取配置
	kv.refreshConf()
	// 之后保持每100ms刷新一次
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

func genSnapshot(e *labgob.LabEncoder, args ...interface{}) error {
	for _, arg := range args {
		err := e.Encode(arg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	genSnapshot(e, kv.data, kv.lastReq, kv.config, kv.getAble, kv.putAble)
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
	var conf shardctrler.Config
	err = d.Decode(&conf)
	if err != nil {
		panic(err)
	}
	var getAble []int32
	err = d.Decode(&getAble)
	if err != nil {
		panic(err)
	}
	var putAble int32
	err = d.Decode(&putAble)
	if err != nil {
		panic(err)
	}
	kv.lock()
	defer kv.unlock()
	kv.data = data
	kv.lastReq = lastReq
	kv.config = conf
	kv.getAble = getAble
	kv.putAble = putAble
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.lock()
	defer kv.unlock()
	// 不可GET
	if !(kv.getAble[shard] == 1 && kv.confAble == 1 && atomic.LoadInt32(&kv.applyAble) == 1) {
		reply.Err = ErrNoAble
		// Dprintf("[ShardKV-%d-%d] getAble=%v\n", kv.gid, kv.me, kv.getAble)
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
	// 不可PUT和APPEND
	if !(kv.putAble == 1 && kv.confAble == 1 && atomic.LoadInt32(&kv.applyAble) == 1) {
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
	// confAble == 1则说明conf已经更新完成
	if kv.confAble == 1 || atomic.LoadInt32(&kv.applyAble) != 1 {
		reply.Err = ErrNoAble
		return
	}
	_, _, is_leader := kv.rf.Start(Op{
		ClientId: int64(args.From),
		ReqId:    args.ReqId,
		Args:     TransformData{Shard: args.Shard, Data: args.Data},
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
	if reply.Err == OK {
		fmt.Printf("[ShardKV-%d-%d] 接收 Shard-%d 数据 完成, kv.getAble=%v\n", kv.gid, kv.me, args.Shard, kv.getAble)
	}
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(TransformData{})

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
	kv.putAble = 0
	kv.getAble = make([]int32, shardctrler.NShards)
	kv.applyAble = 0

	fmt.Printf("[ShardKV-%d-%d] 启动, 需要重放 %d 条日志\n", kv.gid, kv.me, kv.rf.GetCommitIdx())

	go kv.handleApply()
	go kv.refreshTask()

	return kv
}
