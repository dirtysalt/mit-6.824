package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

func SleepMills(v int) {
	time.Sleep(time.Duration(v) * time.Millisecond)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int32
	RequestId int32
	RpcId     int32
}

func (p *PutAppendArgs) String() string {
	return fmt.Sprintf("req(clientId=%d, reqId=%d, rpcId=%d, key=%s, value=%s, op=%s)",
		p.ClientId, p.RequestId, p.RpcId, p.Key, p.Value, p.Op)
}

type PutAppendReply struct {
	Err Err
}

func (p *PutAppendReply) String() string {
	return fmt.Sprintf("reply(err=%s)", p.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int32
	RequestId int32
	RpcId     int32
}

func (p *GetArgs) String() string {
	return fmt.Sprintf("req(clientId=%d, reqId=%d, rpcId=%d, key=%s)",
		p.ClientId, p.RequestId, p.RpcId, p.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (p *GetReply) String() string {
	return fmt.Sprintf("reply(err=%s, value=%s)",
		p.Err, p.Value)
}
