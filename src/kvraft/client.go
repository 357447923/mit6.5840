package kvraft

import (
	"6.5840/labrpc"
	"log"
	"strconv"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	ChangeLeaderInterval = 50 * time.Millisecond
	RequestTimeOut       = 500 * time.Millisecond
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	leaderId int // leaderEntry cache
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClientId: ck.clientId,
		ReqId:    nrand(),
		Key:      key,
	}
	// You will have to modify this function.
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		ok := false
		success := make(chan bool)
		go func() {
			hasRecv := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
			success <- hasRecv
		}()
		stop := time.NewTimer(RequestTimeOut)
		select {
		case <-stop.C:
			reply.Err = ErrTimeOut
			ok = false
		case ok = <-success:
		}
		if !ok {
			DPrintf("client=%v req server=%d not ok\n", ck.clientId, leaderId)
		} else if reply.Err != OK {
			DPrintf("client=%v req server=%d err=%v\n", ck.clientId, leaderId, reply.Err)
		}
		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			time.Sleep(ChangeLeaderInterval)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			DPrintf("client=%v get key=%s err=%v\n", ck.clientId, key, ErrNoKey)
			return ""
		case ErrTimeOut:
			time.Sleep(ChangeLeaderInterval)
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	start := time.Now().UnixMilli()
	defer func(start int64) {
		DPrintf("client=%d req success. cost=%vms\n", ck.clientId, time.Now().UnixMilli()-start)
	}(start)
	// You will have to modify this function.
	args := PutAppendArgs{
		ClientId: ck.clientId,
		ReqId:    nrand(),
		Key:      key,
		Value:    value,
		Op:       op,
	}
	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := false
		success := make(chan bool)
		go func() {
			hasRecv := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
			success <- hasRecv
		}()
		stop := time.NewTimer(RequestTimeOut)
		select {
		case <-stop.C:
			reply.Err = ErrTimeOut
			ok = false
		case ok = <-success:
		}
		if !ok {
			DPrintf("client=%v req server=%d not ok\n", ck.clientId, leaderId)
		} else if reply.Err != OK {
			DPrintf("client=%v req server=%d err=%v\n", ck.clientId, leaderId, reply.Err)
		}
		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			DPrintf("client=%v put append {\"%s\":\"%s\"} success\n", ck.clientId, key, value)
			ck.leaderId = leaderId
			return
		case ErrNoKey:
			log.Fatal("client" + strconv.Itoa(int(ck.clientId)) + "putappend get err nokey\n")
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			time.Sleep(ChangeLeaderInterval)
			continue
		default:
			log.Fatal("client unknown err ", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
