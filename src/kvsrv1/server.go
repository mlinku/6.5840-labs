package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	mu sync.Mutex
	kv_store map[string]kvEntry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kv_store: make(map[string]kvEntry),
	}

	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.kv_store[key]
	if exists {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return 
	// Your code here.
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	version := args.Version
	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.kv_store[key]
	if exists {
		// key exists
		if version != entry.Version {
			// version mismatch
			reply.Err = rpc.ErrVersion
			return
		} else {
			// version match, update value and increment version
			entry.Value = value
			entry.Version++
			kv.kv_store[key] = entry
			reply.Err = rpc.OK
			return
		}
	} else {
		// key does not exist
		if version != 0 {
			// version mismatch
			reply.Err = rpc.ErrNoKey
			return
		} else {
			// install new key with version 1
			kv.kv_store[key] = kvEntry{Value: value, Version: 1}
			reply.Err = rpc.OK
			return
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
