package rsm

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1
var (
	dbgLogger *log.Logger
	dbgOnce   sync.Once
)

func initLogger() {
	dbgOnce.Do(func() {
		f, err := os.OpenFile("/home/mwl/6.5840-labs/src/kvraft1/rsm/kvraft-rsm.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			dbgLogger = log.New(os.Stderr, "rsm ", log.LstdFlags|log.Lmicroseconds)
			dbgLogger.Printf("[logger] open file failed: %v; use stderr", err)
			return
		}
		dbgLogger = log.New(f, "rsm ", log.LstdFlags|log.Lmicroseconds)
		dbgLogger.Printf("[logger] init file logger ok")
	})
	if dbgLogger == nil {
		dbgLogger = log.New(os.Stderr, "rsm ", log.LstdFlags|log.Lmicroseconds)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

type taskInfo struct {
	index int
	term  int
}

type resultInfo struct {
	res any
	op  Op
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// Your definitions here.
	reqIndex        int
	reqChanMap      map[taskInfo]chan resultInfo
	timeOutInterval time.Duration
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	initLogger()
	rsm := &RSM{
		me:              me,
		maxraftstate:    maxraftstate,
		applyCh:         make(chan raftapi.ApplyMsg),
		sm:              sm,
		reqIndex:        0,
		timeOutInterval: 2 * time.Second, // was 1s; keep > test window
		reqChanMap:      map[taskInfo]chan resultInfo{},
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) applier() {
	initLogger()
	for {
		msg := <-rsm.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			dbgLogger.Printf("[applier] me=%d apply idx=%d term=%d op.me=%d op.id=%d", rsm.me, msg.CommandIndex, msg.CommandTerm, op.Me, op.Id)
			res := resultInfo{res: rsm.sm.DoOp(op.Req), op: op}

			rsm.mu.Lock()
			if ch, ok := rsm.reqChanMap[taskInfo{index: msg.CommandIndex, term: msg.CommandTerm}]; ok {
				dbgLogger.Printf("[applier] me=%d send result idx=%d term=%d op.me=%d op.id=%d", rsm.me, msg.CommandIndex, msg.CommandTerm, op.Me, op.Id)
				select {
				case ch <- res:
				default:
				}
			}
			rsm.mu.Unlock()
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	initLogger()

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// Id consist of me and rsm.reqIndex
	rsm.mu.Lock()
	rsm.reqIndex++
	op := Op{Me: rsm.me, Id: time.Now().UnixNano() + rand.Int63(), Req: req}
	dbgLogger.Printf("[submit] me=%d start op.id=%d req=%T", rsm.me, op.Id, req)
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		dbgLogger.Printf("[submit] me=%d not leader op.id=%d", rsm.me, op.Id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// wait for the command to be applied
	ti := taskInfo{index: index, term: term}
	ch := make(chan resultInfo, 1)
	rsm.reqChanMap[ti] = ch
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		delete(rsm.reqChanMap, ti)
		rsm.mu.Unlock()
	}()

	var result any
	var err rpc.Err = rpc.OK
	// wait for the result or timeout
	select {
	case ri := <-ch:
		currTerm, isLeader := rsm.rf.GetState()
		if ri.op.Me != op.Me || ri.op.Id != op.Id || !isLeader || currTerm != term {
			dbgLogger.Printf("[submit] mismatch me=%d op.id=%d ri.op.id=%d isLeader=%v currTerm=%d wantTerm=%d", rsm.me, op.Id, ri.op.Id, isLeader, currTerm, term)
			err = rpc.ErrWrongLeader
		} else {
			dbgLogger.Printf("[submit] ok me=%d op.id=%d idx=%d term=%d", rsm.me, op.Id, ti.index, ti.term)
			result = ri.res
		}
	case <-time.After(rsm.timeOutInterval):
		dbgLogger.Printf("[submit] timeout me=%d op.id=%d idx=%d term=%d", rsm.me, op.Id, ti.index, ti.term)
		err = rpc.ErrWrongLeader

	}

	return err, result

}
