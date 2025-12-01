package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object defines each log
type LogEntry struct { 
    Index   int         
    Term    int         
    Command interface{} 
}

type serverState int
const(
	stateFollower serverState = iota
	stateCandidate
	stateLeader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     serverState         // server state: follower, candidate, leader
	// Your data here (3A, 3B, 3C).
	// 3A
	currentTerm 	  int // current term of 
	votedFor	  	  int // candidateId that received vote in current term (or null if none)
	log				  []LogEntry // log each
	commitIndex	  int // index of highest log entry known to be committed
	lastRPCtime	  time.Time // time of last RPC received
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == stateLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term 	  int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	
	// Your data here (3A).
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate’s log is at least as up-to-date as receiver’s log, grant vote
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		
		if args.LastLogTerm > lastLogTerm || 
           (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastRPCtime = time.Now()
			return
		}
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    if args.Term > rf.currentTerm || (rf.state == stateCandidate && args.Term == rf.currentTerm) {
        rf.currentTerm = args.Term
        rf.state = stateFollower
        rf.votedFor = -1
    }

    // 收到 leader 的心跳，重置选举计时器
    rf.lastRPCtime = time.Now()

    reply.Term = rf.currentTerm
    reply.Success = true

    // 3B 以后再处理日志匹配等问题
}



// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// Placeholder for any election initialization logic
	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = stateCandidate
	rf.lastRPCtime = time.Now()
	voteCount := 1 // vote for self

	// send RequestVote RPCs to all other servers
	requestVoteArgs := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, requestVoteArgs, requestVoteReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// process reply 必须处理term更大的情况，并及时更新
				if requestVoteReply.Term > rf.currentTerm {
					rf.currentTerm = requestVoteReply.Term
					rf.state = stateFollower
					rf.votedFor = -1
					rf.lastRPCtime = time.Now()
					return
				}

				if rf.state != stateCandidate || rf.currentTerm != requestVoteArgs.Term{
					// no longer candidate or term changed
					return
				}

				if requestVoteReply.VoteGranted {
					// count votes
					voteCount++
					// if votes received from majority of servers: become leader
					if voteCount > len(rf.peers)/2 {
						rf.state = stateLeader
						// initialize leader state
						// ...
						// send initial empty AppendEntries RPCs (heartbeats) to each server
						go rf.broadcastHeartbeat()
					}
					return
				}
			}
		}(peer)

	}	
}


func (rf *Raft) broadcastHeartbeat() {
    rf.mu.Lock()
    if rf.state != stateLeader {
        rf.mu.Unlock()
        return
    }
    
    // 1. 准备参数（在锁内一次性完成）
    args := &AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: len(rf.log) - 1,
        PrevLogTerm:  rf.log[len(rf.log)-1].Term,
        Entries:      nil, // 心跳为空
        LeaderCommit: rf.commitIndex,
    }
    rf.mu.Unlock() // 构造完立刻解锁

    // 2. 并行发送
    for peer := range rf.peers {
        if peer == rf.me { continue }
        
        go func(server int) {
            reply := &AppendEntriesReply{}
            if rf.sendAppendEntries(server, args, reply) {
                rf.mu.Lock()
                defer rf.mu.Unlock()
                
                // 处理回复
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = stateFollower
                    rf.votedFor = -1
                }
            }
        }(peer)
    }
}

func (rf *Raft) ticker() {
    for rf.killed() == false {
        time.Sleep(100 * time.Millisecond)

        rf.mu.Lock()
        state := rf.state
        lastRPC := rf.lastRPCtime
        rf.mu.Unlock()

        if state == stateLeader {
            rf.broadcastHeartbeat()
        } else {
            // 生成随机超时 (300~450ms)
            timeout := time.Duration(300 + (rand.Int63() % 150)) * time.Millisecond
            if time.Since(lastRPC) > timeout {
                rf.startElection()
            }
        }
    }
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil}) // dummy log entry at index 0
	rf.state = stateFollower
	rf.lastRPCtime = time.Now()	
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
