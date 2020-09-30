package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	RequestId int32
}

func (p *PutAppendArgs) String() string {
	return fmt.Sprintf("put-req(clientId=%d, reqId=%d, key=%s, value=%s, op=%s)",
		p.ClientId, p.RequestId, p.Key, p.Value, p.Op)
}

type PutAppendReply struct {
	Err       Err
	LeaderIdx int
}

func (p *PutAppendReply) String() string {
	return fmt.Sprintf("put-reply(err=%s, leaderIdx=%d)",
		p.Err, p.LeaderIdx)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int
	RequestId int32
}

func (p *GetArgs) String() string {
	return fmt.Sprintf("req(clientId=%d, reqId=%d, key=%s)",
		p.ClientId, p.RequestId, p.Key)
}

type GetReply struct {
	Err       Err
	LeaderIdx int
	Value     string
}

func (p *GetReply) String() string {
	return fmt.Sprintf("put-reply(err=%s, leaderIdx=%d, value=%s)",
		p.Err, p.LeaderIdx, p.Value)
}
