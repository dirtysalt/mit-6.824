package kvraft

import (
	"bytes"
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
	applyTime  time.Time
	rpcTrace   map[int32]time.Time
	ansBuffer  map[int32]chan *ApplyAnswer
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

type ApplyAnswer struct {
	Term  int
	Index int
	Err   Err
	Value string
}

func (kv *KVServer) EnterRpc(rpcId int32) {
	kv.Lock()
	defer kv.Unlock()
	kv.rpcTrace[rpcId] = time.Now()
}

func (kv *KVServer) ExitRpc(rpcId int32) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.rpcTrace, rpcId)
}

func (kv *KVServer) CreateRpcChan(rpcId int32) chan *ApplyAnswer {
	ch := make(chan *ApplyAnswer, 1)
	kv.Lock()
	defer kv.Unlock()
	kv.ansBuffer[rpcId] = ch
	return ch
}

func (kv *KVServer) DeleteRpcChan(rpcId int32) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.ansBuffer, rpcId)
}

func (kv *KVServer) SubmitAnswer(msg *raft.ApplyMsg, ans *ApplyAnswer) {
	kv.Lock()
	defer kv.Unlock()

	kv.applyIndex = msg.CommandIndex
	kv.applyTime = time.Now()

	op := msg.Command.(Op)
	if op.ServerId == kv.me {
		rpcId := op.RpcId
		ch := kv.ansBuffer[rpcId]
		if ch != nil {
			ch <- ans
		}
	}
}

func (kv *KVServer) WaitAnswer(index int, term int, ch chan *ApplyAnswer) *ApplyAnswer {
	for {
		if kv.killed() {
			return nil
		}

		currentTerm, _ := kv.rf.GetState()
		if currentTerm != term {
			return nil
		}

		select {
		case ans := <-ch:
			{
				return ans
			}
		case <-time.After(500 * time.Millisecond):
			{
			}
		}
	}
}

func (kv *KVServer) CallAndWait(op *Op) (err Err, ans string) {
	err, ans = ErrWrongLeader, ""

	rpcId := op.RpcId
	kv.EnterRpc(rpcId)
	defer kv.ExitRpc(rpcId)
	defer func() {
		DPrintf("kv%d: return rpc#%d -> reply(%s,'%s')", kv.me, rpcId, err, ans)
	}()

	ch := kv.CreateRpcChan(rpcId)
	defer kv.DeleteRpcChan(rpcId)

	index, term, isLeader := kv.rf.Start(*op)
	DPrintf("kv%d: start rpc#%d -> reply(term=%d, index=%d, isLeader=%v)", kv.me, rpcId, term, index, isLeader)
	if !isLeader {
		return
	}

	res := kv.WaitAnswer(index, term, ch)
	if res == nil {
		return
	}
	if res.Index != index {
		panic(fmt.Sprintf("command index disagree. res = %d, exp = %d", res.Index, index))
	}
	if res.Term != term {
		msg := fmt.Sprintf("kv%d: %s -> term disagree. res = %d, exp = %d", kv.me, op, res.Term, term)
		DPrintf(msg)
	} else {
		err = res.Err
		ans = res.Value
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
	err, value := kv.CallAndWait(&op)
	reply.Err = err
	reply.Value = value
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
	err, _ := kv.CallAndWait(&op)
	reply.Err = err
}

func (kv *KVServer) applyOp(op *Op) (err Err, ans string) {
	ans = ""
	err = OK

	key := op.Key
	value := op.Value
	cmd := op.Cmd

	// dedup first
	clientId := op.ClientId
	requestId := op.RequestId
	p, ok := kv.dedup[clientId]
	if ok && p == requestId {
		if cmd == OpGet {
			p, ok := kv.data[key]
			if !ok {
				p = ""
			}
			ans = p
		}
		DPrintf("kv%d: duplicated message(clientId=%d, requestId=%d)", kv.me, clientId, requestId)
		return
	}
	kv.dedup[clientId] = requestId

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
			kv.data[key] = p
		}
	case OpPut:
		{
			kv.data[key] = value
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
			op := msg.Command.(Op)
			byMe := (op.ServerId == kv.me)
			if byMe {
				DPrintf("kv%d: apply message %v, term = %d, index = %d", kv.me, &op, msg.CommandTerm, msg.CommandIndex)
			}

			err, value := kv.applyOp(&op)
			// // if byMe {
			// DPrintf("kv%d: data = %v", kv.me, kv.data)
			// // }

			ans := ApplyAnswer{
				Term:  msg.CommandTerm,
				Index: msg.CommandIndex,
				Err:   err,
				Value: value,
			}

			kv.SubmitAnswer(&msg, &ans)

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

func (kv *KVServer) writeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.dedup)
	e.Encode(kv.applyIndex)
	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.dedup)
	d.Decode(&kv.applyIndex)
}

func (kv *KVServer) logCompactionWorker() {
	if kv.maxraftstate == -1 {
		return
	}

	const COMPACTION_RATIO = 0.8
	for {
		if kv.killed() {
			break
		}
		size := kv.persister.RaftStateSize()
		if float64(size) > float64(kv.maxraftstate)*COMPACTION_RATIO {
			DPrintf("kv%d: make log compaction", kv.me)
			kv.Lock()
			snapshot := kv.writeSnapshot()
			applyIndex := kv.applyIndex
			kv.Unlock()
			kv.rf.WriteSnapshot(snapshot, applyIndex)
		}
		SleepMills(MaxWaitTime)
	}
}

func (kv *KVServer) checkRpcTrace() {
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
		dur := now.Sub(kv.applyTime)
		if dur.Milliseconds() > MaxWaitTime {
			DPrintf("kv%d: no message committed too long", kv.me)
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
	kv.ansBuffer = make(map[int32]chan *ApplyAnswer)
	kv.applyTime = time.Now()

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applyWorker()
	go kv.checkRpcTrace()
	go kv.logCompactionWorker()
	return kv
}
