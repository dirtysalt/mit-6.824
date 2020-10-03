package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	MaxWaitTime = 5000
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd       string
	Key       string
	Value     string
	ServerId  int
	RpcId     int32
	ClientId  int32
	RequestId int32
}

func (op *Op) String() string {
	return fmt.Sprintf("op(cmd=%s, key=%s, value='%s', serverId=%d, rpcId=%d, clientId=%d, requestId=%d)",
		op.Cmd, op.Key, op.Value, op.ServerId, op.RpcId, op.ClientId, op.RequestId)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data       map[string]string
	dedup      map[int32]int32
	applyIndex int
	rpcTrace   map[int32]time.Time
	rpcBinding map[int32]chan ApplyResult
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

type ApplyResult struct {
	Term  int
	Index int
	Value string
}

func (kv *KVServer) AddRpcChan(rpcId int32) chan ApplyResult {
	kv.Lock()
	defer kv.Unlock()

	output := make(chan ApplyResult, 1)
	_, ok := kv.rpcBinding[rpcId]
	if ok {
		panic(fmt.Sprintf("kv%d: rpcId#%d has already been added", kv.me, rpcId))
	}
	kv.rpcBinding[rpcId] = output
	kv.rpcTrace[rpcId] = time.Now()
	return output
}

func (kv *KVServer) DelRpcChan(rpcId int32) {
	kv.Lock()
	defer kv.Unlock()

	_, ok := kv.rpcBinding[rpcId]
	if !ok {
		panic(fmt.Sprintf("rpcId#%d has already been deleted", rpcId))
	}
	delete(kv.rpcBinding, rpcId)
	delete(kv.rpcTrace, rpcId)
}

func (kv *KVServer) CallAndWait(op *Op) (ok bool, ans string) {
	ok, ans = false, ""

	rpcId := op.RpcId
	output := kv.AddRpcChan(rpcId)
	defer kv.DelRpcChan(rpcId)

	DPrintf("kv%d: start command %s", kv.me, op)
	index, term, isLeader := kv.rf.Start(*op)
	DPrintf("kv%d: rpc#%d -> reply(term=%d, index=%d, isLeader=%v)", kv.me, rpcId, term, index, isLeader)
	if !isLeader {
		return
	}

	select {
	case res := <-output:
		{
			if res.Index != index {
				panic(fmt.Sprintf("command index disagree. res = %d, exp = %d", res.Index, index))
			}
			if res.Term != term {
				DPrintf("kv%d: %s -> term disagree. res = %d, exp = %d", kv.me, op, res.Term, term)
				return
			}
			ok = true
			ans = res.Value
		}
	case <-time.After(MaxWaitTime * time.Millisecond):
		{
			DPrintf("kv%d: timeout!!!!", kv.me)
		}
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	rpcId := args.RpcId
	op := Op{
		Cmd:       OpGet,
		Key:       args.Key,
		ServerId:  kv.me,
		RpcId:     rpcId,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, value := kv.CallAndWait(&op)
	reply.Err = ErrWrongLeader
	if ok {
		reply.Err = OK
		reply.Value = value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	rpcId := args.RpcId
	op := Op{
		Cmd:       args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ServerId:  kv.me,
		RpcId:     rpcId,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, _ := kv.CallAndWait(&op)
	reply.Err = ErrWrongLeader
	if ok {
		reply.Err = OK
	}
}

func (kv *KVServer) applyOp(op *Op) (ans string) {
	ans = ""
	// dedup first
	{
		clientId := op.ClientId
		requestId := op.RequestId
		p, ok := kv.dedup[clientId]
		if ok && p == requestId {
			DPrintf("kv%d: duplicated message(clientId=%d, requestId=%d)", kv.me, clientId, requestId)
			return
		}
		kv.dedup[clientId] = requestId
	}
	key := op.Key
	value := op.Value
	cmd := op.Cmd

	switch cmd {
	case OpGet:
		{
			p, ok := kv.data[key]
			if !ok {
				p = ""
			}
			ans = p
		}
	case OpAppend:
		{
			p, ok := kv.data[key]
			if !ok {
				p = ""
			}
			p = p + value
			kv.data[op.Key] = p
		}
	case OpPut:
		{
			kv.data[op.Key] = value
		}
	default:
		panic(fmt.Sprintf("unknown cmd: %s", cmd))
	}
	return
}

func (kv *KVServer) applyWorker() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			// TODO: apply
			op := msg.Command.(Op)
			byMe := (op.ServerId == kv.me)
			if byMe {
				DPrintf("kv%d: apply message %v", kv.me, &msg)
			}

			value := kv.applyOp(&op)
			kv.applyIndex = msg.CommandIndex

			rpcId := op.RpcId
			kv.Lock()
			ch, ok := kv.rpcBinding[rpcId]
			kv.Unlock()
			res := ApplyResult{
				Term:  msg.CommandTerm,
				Index: msg.CommandIndex,
				Value: value,
			}
			if ok {
				ch <- res
			} else {
				if byMe {
					DPrintf("kv%d: rpc%d channel not exist", kv.me, rpcId)
				}
			}
		} else {
			DPrintf("kv%d: apply message %v", kv.me, &msg)
			data := msg.Command.(string)
			if data == "kill" {
				DPrintf("kv%d: kill apply worker", kv.me)
				break
			}
		}
	}
}

func (kv *KVServer) checkOpTrace() {
	for {
		if kv.killed() {
			break
		}
		kv.Lock()
		now := time.Now()
		for k, v := range kv.rpcTrace {
			dur := now.Sub(v)
			if dur.Milliseconds() > MaxWaitTime {
				DPrintf("kv%d: rpc#%d waits too long. rf.lockAt = %d", kv.me, k, kv.rf.GetLockAt())
			}
		}
		kv.Unlock()
		SleepMills(MaxWaitTime)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	msg := raft.ApplyMsg{
		CommandValid: false,
		Command:      "kill",
	}
	kv.applyCh <- msg
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
// for any long-running work.oo
//
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

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.dedup = make(map[int32]int32)
	kv.rpcTrace = make(map[int32]time.Time)
	kv.rpcBinding = make(map[int32]chan ApplyResult)
	go kv.applyWorker()
	go kv.checkOpTrace()
	return kv
}
