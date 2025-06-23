package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const DEBUG = false

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrTimeout     = "ErrTimeout"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoAble      = "ErrNoAble"
	ErrConfUpdate  = "ErrConfUpdate"
)

const (
	Get       = "Get"
	Put       = "Put"
	Append    = "Append"
	Transform = "Transform"
)

type ReqId int64
type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	ReqId    ReqId
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	ReqId    ReqId
}

type GetReply struct {
	Err   Err
	Value string
}

type ServerPutArgs struct {
	From      int
	ReqId     ReqId
	Shard     int
	ConfigNum int
	Data      map[string]string
}

type ServerPutReply struct {
	Err Err
}

func Dprintf(ft string, a ...interface{}) {
	if DEBUG {
		fmt.Printf(ft, a...)
	}
}
