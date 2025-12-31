package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu       sync.Mutex
	kv_store map[string]kvEntry
}

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type Req struct {
	// Your definitions here.
	ReqType int // 0 for Get, 1 for Put
	Key     string
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	if r, ok := req.(Req); ok {
		switch r.ReqType {
		case 0: // Get
			kv.mu.Lock()
			defer kv.mu.Unlock()
			entry, exists := kv.kv_store[r.Key]
			if exists {
				return rpc.GetReply{Err: rpc.OK, Value: entry.Value, Version: entry.Version}
			} else {
				return rpc.GetReply{Err: rpc.ErrNoKey}
			}
		case 1: // Put
			kv.mu.Lock()
			defer kv.mu.Unlock()
			entry, exists := kv.kv_store[r.Key]
			if exists {
				// key exists
				if r.Version != entry.Version {
					// version mismatch
					return rpc.PutReply{Err: rpc.ErrVersion}
				} else {
					// version match, update value and increment version
					entry.Value = r.Value
					entry.Version++
					kv.kv_store[r.Key] = entry
					return rpc.PutReply{Err: rpc.OK}
				}
			} else {
				// key does not exist
				if r.Version != 0 {
					// version mismatch
					return rpc.PutReply{Err: rpc.ErrNoKey}
				} else {
					// install new key with version 1
					kv.kv_store[r.Key] = kvEntry{Value: r.Value, Version: 1}
					return rpc.PutReply{Err: rpc.OK}
				}
			}
		}
		return nil
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kv_store)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kv_store map[string]kvEntry

	if d.Decode(&kv_store) != nil {
		// handle error
	} else {
		kv.mu.Lock()
		kv.kv_store = kv_store
		// kv.duplicateTable = ...
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := Req{
		ReqType: 0,
		Key:     args.Key,
	}
	err, value := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if entry, ok := value.(rpc.GetReply); ok {
		reply.Err = entry.Err
		reply.Value = entry.Value
		reply.Version = entry.Version
	}

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := Req{
		ReqType: 1,
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
		// 记得之后加上 ClientId 和 RequestId
	}
	err, value := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if putReply, ok := value.(rpc.PutReply); ok {
		reply.Err = putReply.Err
		return
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(Req{})

	kv := &KVServer{me: me, mu: sync.Mutex{}, kv_store: make(map[string]kvEntry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
