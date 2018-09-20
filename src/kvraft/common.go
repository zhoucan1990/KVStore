package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"

	Append = "Append"
	Put    = "Put"
	Get    = "Get"
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
	Cid int64
	Seq int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type getBgReq struct {
	req  *GetArgs
	done chan *bgResp
}

type updateBgReq struct {
	req  *PutAppendArgs
	done chan *bgResp
}

type bgResp struct {
	wrongLeader bool
	err         Err
	value       string
}

type raftAckMsg struct {
	index int
	term  int
}

type session struct {
	sop  *Op         //op from caller
	ack  *raftAckMsg //raft commit1 ack
	done chan *bgResp
}

type ValueRepo struct {
	Values []*Value
	Start  int
}

type Value struct {
	Value   string
	Version string
}

func (v *Value) String() string {
	return v.Value
}
