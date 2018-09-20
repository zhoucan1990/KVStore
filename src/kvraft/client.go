package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
	"time"
)

var sleepInterval = 1 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	cid    int64
	seq    int64
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
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Get... %s", key)
	args := GetArgs{
		Key: key,
	}
	l := len(ck.servers)
	i := ck.leader % l
	value := ""
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			i = (i + 1) % l
			continue
		}
		if reply.Err != "" {
			if reply.Err == ErrNoKey {
				return ""
			}
		}
		value = reply.Value
		break
	}
	ck.leader = i
	DPrintf("Get value: %s", value)
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	seq := atomic.AddInt64(&ck.seq, 1)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Cid:   ck.cid,
		Seq:   seq,
	}
	l := len(ck.servers)
	i := ck.leader % l
	for {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			i = (i + 1) % l
			DPrintf("Retry for version %d\n", args.Seq)
			continue
		}
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
