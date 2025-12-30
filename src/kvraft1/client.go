package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
	mu       sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderId: 0, mu: sync.Mutex{}}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{}
	args.Key = key
	serverId := ck.servers[ck.leaderId]
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(serverId, "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongLeader:
				ck.mu.Lock()
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				serverId = ck.servers[ck.leaderId]
				ck.mu.Unlock()
				continue
			default:
				return "", 0, reply.Err
			}
		}
		// rpc 失败，尝试下一个 server
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		serverId = ck.servers[ck.leaderId]
		ck.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{}
	args.Key = key
	args.Value = value
	args.Version = version
	serverId := ck.servers[ck.leaderId]
	times := 0
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(serverId, "KVServer.Put", &args, &reply)
		times += 1
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrWrongLeader:
				ck.mu.Lock()
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				serverId = ck.servers[ck.leaderId]
				ck.mu.Unlock()
				continue
			case rpc.ErrVersion:
				if times == 1 {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			default:
				return reply.Err
			}
		}
		// rpc 失败，尝试下一个 server
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		serverId = ck.servers[ck.leaderId]
		ck.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
}
