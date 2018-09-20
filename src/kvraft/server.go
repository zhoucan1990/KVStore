package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"os"
	"raft"
	"sync"
	"time"
)

const Debug = 0

var tickInterval = 800 * time.Millisecond

func init() {
	log.SetOutput(os.Stdout)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
	Cid   int64
	Seq   int64
}

type RaftKV struct {
	mu sync.Mutex
	me int
	rf *raft.Raft

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	close     chan struct{}
	sessionCh chan *session
	applyCh   chan raft.ApplyMsg
	cpCh      chan struct{}

	sessionMap map[int]*session // raft index to hanging session
	requests   map[int64]int64  // client id to sequence for dedup
	kvStore    map[string]string

	inSnap    bool
	curIdx    int
	persister *raft.Persister
}

func (kv *RaftKV) makeSnapshot() {
	if kv.inSnap {
		return
	}
	kv.inSnap = true
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	e.Encode(kv.requests)
	e.Encode(kv.kvStore)
	go func(idx int) {
		kv.rf.MakeSnapShot(idx, b.Bytes())
		kv.inSnap = false
	}(kv.curIdx)
}

func (kv *RaftKV) readSnapshot(snapshot []byte, idx int) {
	if kv.curIdx >= idx {
		return
	}
	if len(snapshot) == 0 {
		return
	}
	b := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(b)
	req := make(map[int64]int64)
	store := make(map[string]string)
	d.Decode(&req)
	d.Decode(&store)
	kv.requests = req
	kv.kvStore = store
	kv.curIdx = idx
}

func (kv *RaftKV) bgLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.applyMsg(msg)
		case ev := <-kv.sessionCh:
			DPrintf("Receive session event: %v", ev)
			kv.sessionMap[ev.ack.index] = ev
		case <-kv.cpCh:
			kv.makeSnapshot()
		case <-kv.close:
			kv.closeSession()
			return
		}
	}
}

func (kv *RaftKV) closeSession() {
	for _, s := range kv.sessionMap {
		close(s.done)
	}
}

func (kv *RaftKV) execute(op *Op) {
	if op.Type == Append {
		kv.kvStore[op.Key] += op.Value
	} else if op.Type == Put {
		kv.kvStore[op.Key] = op.Value
	}
}

func (kv *RaftKV) applyMsg(msg raft.ApplyMsg) {
	if msg.UseSnapshot {
		kv.readSnapshot(msg.Snapshot, msg.Index)
		return
	}
	op, ok := msg.Command.(Op)
	if !ok {
		return
	}
	kv.curIdx = msg.Index
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		go func() {
			kv.cpCh <- struct{}{}
		}()
	}
	switch op.Type {
	case Append:
		fallthrough
	case Put:
		DPrintf("commit put key:%s value:%s id:%d seq %d index:%d\n", op.Key, op.Value, op.Cid, op.Seq, msg.Index)
		if seq, ok := kv.requests[op.Cid]; !ok || seq < op.Seq {
			kv.execute(&op)
			kv.requests[op.Cid] = op.Seq
		}
		kv.replyBack(&op, msg.Index, "")
	case Get:
		DPrintf("commit get key:%s id:%d seq:%d index:%d\n", op.Key, op.Cid, op.Seq, msg.Index)
		kv.replyBack(&op, msg.Index, kv.kvStore[op.Key])
	default:
		return
	}
}

func (kv *RaftKV) replyBack(op *Op, index int, value string) {
	if s, ok := kv.sessionMap[index]; ok {
		delete(kv.sessionMap, index)
		reply := &bgResp{}
		if *s.sop != *op {
			reply.wrongLeader = true
		} else {
			reply.value = value
		}
		select {
		case s.done <- reply:
		default:
		}
	}
}

func (kv *RaftKV) sendOp(op *Op) *bgResp {
	var ret *bgResp
	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return &bgResp{wrongLeader: true}
	}
	done := make(chan *bgResp)
	kv.sessionCh <- &session{
		sop:  op,
		ack:  &raftAckMsg{index: index, term: term},
		done: done,
	}
	select {
	case ret = <-done:
	case <-time.After(tickInterval):
		break
	}
	return ret
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:  args.Key,
		Type: Get,
	}
	ret := kv.sendOp(&op)
	if ret == nil {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = ret.wrongLeader
		reply.Err = ret.err
		reply.Value = ret.value
	}
	DPrintf("Get done %s %v\n", args.Key, reply)
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Put %s %s %s\n", args.Key, args.Value, args.Op)
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Type:  args.Op,
		Cid:   args.Cid,
		Seq:   args.Seq,
	}
	ret := kv.sendOp(&op)
	if ret == nil {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = ret.wrongLeader
		reply.Err = ret.err
	}
	DPrintf("Put done %s %s %v\n", args.Key, args.Value, reply)
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	DPrintf("metric rf call start on kill\n")
	kv.rf.Kill()
	close(kv.close)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.close = make(chan struct{})
	kv.sessionMap = make(map[int]*session)
	kv.requests = make(map[int64]int64)
	kv.kvStore = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.sessionCh = make(chan *session, 128)
	kv.cpCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	// You may need initialization code here.
	go kv.bgLoop()
	return kv
}
