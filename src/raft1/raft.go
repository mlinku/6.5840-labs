package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Debugging
const Debug_raft = true

// 全局 Logger，用于写入文件
var DLog *log.Logger

func init() {
	if !Debug_raft {
		return
	}
	// 打开或创建 raft-debug.log 文件，追加模式
	f, err := os.OpenFile("raft-debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Failed to open raft-debug.log: %v\n", err)
		return
	}
	// 初始化 logger，包含时间戳
	DLog = log.New(f, "", log.LstdFlags|log.Lmicroseconds)

}

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
	applyCh chan raftapi.ApplyMsg
	replicatorCond []chan struct{} // condition variables for each follower's log replicator goroutine

	// Your data here (3A, 3B, 3C).
	// 3A
	currentTerm 	  int // current term of 
	votedFor	  	  int // candidateId that received vote in current term (or null if none)
	log				  []LogEntry // log each
	commitIndex	  int // index of highest log entry known to be committed
	lastApplied	  int // index of highest log entry applied to state machine
	lastRPCtime	  time.Time // time of last RPC received
	electionTimeout time.Duration // time to wait before starting election
	rng             *rand.Rand    // 新增专用随机生成器
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	nextIndex	  []int // for each server, index of the next log entry to send to that server
	matchIndex	  []int // for each server, index of highest log entry known to be replicated on server
}

func (rf *Raft) resetElectionTimer() {
	rf.lastRPCtime = time.Now()
	ms := 500 + (rf.rng.Int63() % 300)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int  
	var votedFor int
	var log []LogEntry
	// var xxx
	// var yyy
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
	  panic("failed to read persisted state")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = log
	  // ...
	}
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

	XTerm  int // for optimization
	XIndex int // for optimization
	XLen   int // for optimization
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Server %d received RequestVote from %d, term %d\n", rf.me, args.CandidateId, args.Term)
	if DLog != nil { DLog.Printf("[Comn] Server %d received RequestVote from %d, term %d\n", rf.me, args.CandidateId, args.Term) }
	isPersist := false
	reply.VoteGranted = false
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.votedFor = -1
		isPersist = true
		// rf.lastRPCtime = time.Now()
		// rf.resetElectionTimer()
		if DLog != nil { DLog.Printf("[Role] Server %d became follower in term %d (RequestVote from %d)\n", rf.me, rf.currentTerm, args.CandidateId) }
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
			rf.resetElectionTimer()
			isPersist = true
			if DLog != nil { DLog.Printf("[Vote] Server %d granted vote to %d in term %d\n", rf.me, args.CandidateId, rf.currentTerm) }
		}else{
			if args.LastLogTerm < lastLogTerm{
				if DLog != nil { DLog.Printf("[Vote] Server %d denied vote to %d in term %d (due to arg.LastLogTerm %d < lastLogTerm %d) \n", rf.me, args.CandidateId, rf.currentTerm, args.LastLogTerm, lastLogTerm) }
			}else{
				if DLog != nil { DLog.Printf("[Vote] Server %d denied vote to %d in term %d (due to arg.LastLogIndex %d < lastLogIndex %d) \n", rf.me, args.CandidateId, rf.currentTerm, args.LastLogIndex, lastLogIndex) }
			}
		}
	}
	if isPersist {
		rf.persist()
	}
	
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
	if DLog != nil { DLog.Printf("[Comn] Server %d received AppendEntries from %d, term %d, PrevLogIndex %d, PrevLogTerm %d, EntriesLen %d, LeaderCommit %d\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit) }
    // 标记是否需要持久化，替代低效的 defer rf.persist()
    persistNeeded := false

    // 1. Term 检查：如果对方 Term 小于我，直接拒绝
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
		if DLog != nil { DLog.Printf("[Comn] Server %d rejected AppendEntries from %d due to stale term %d < %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm) }
        return
    }

    // 2. 收到有效 Leader 的消息，重置心跳时间
    rf.resetElectionTimer()

    // 3. 状态流转：发现更大的 Term，或者自己是 Candidate 收到当前 Term 的 Leader 消消息
    if args.Term > rf.currentTerm || (rf.state == stateCandidate && args.Term == rf.currentTerm) {
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.votedFor = -1 // Term 变更，重置投票
            persistNeeded = true
        }
        rf.state = stateFollower
		if DLog != nil { DLog.Printf("[Role] Server %d became follower from AppendEntries in term %d (Leader %d)\n", rf.me, rf.currentTerm, args.LeaderId) }
    }

    // 4. 一致性检查 (Consistency Check)
    // 检查 PrevLogIndex 是否存在，以及 Term 是否匹配
    if args.PrevLogIndex >= len(rf.log) {
        // Case 1: Follower 的日志太短，找不到 PrevLogIndex
		if DLog != nil { DLog.Printf("[Log] Server %d log too short for AppendEntries. PrevLogIndex: %d, LogLen: %d\n", rf.me, args.PrevLogIndex, len(rf.log)) }
        reply.Success = false
        reply.Term = rf.currentTerm
        reply.XTerm = 0
        reply.XLen = len(rf.log)
        reply.XIndex = 0 // 无意义，占位
        
        if persistNeeded { rf.persist() }
        return
    }

    if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        // Case 2: PrevLogIndex 处存在日志，但 Term 不匹配（冲突）
		if DLog != nil { DLog.Printf("[Log] Server %d conflict in AppendEntries. PrevLogIndex: %d, MyTerm: %d, ArgsTerm: %d\n", rf.me, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm) }
        reply.Success = false
        reply.Term = rf.currentTerm
        
        // 填充加速回溯信息
        conflictTerm := rf.log[args.PrevLogIndex].Term
        reply.XTerm = conflictTerm
        reply.XLen = len(rf.log)
        
        // 寻找 conflictTerm 在本地日志中的第一条索引
        // 这样 Leader 可以一次性回退整个 Term
        index := args.PrevLogIndex
        // 注意：index > 0 保护，不检查 dummy entry (index 0)
        for index > 0 && rf.log[index-1].Term == conflictTerm {
            index--
        }
        reply.XIndex = index

        if persistNeeded { rf.persist() }
        return
    }

	// 5. 日志追加 (Log Appending)
    logChanged := false
    
    // 找到第一个冲突的位置，或者第一个新日志的位置
    appendStartIndex := -1
    for i, entry := range args.Entries {
        idx := args.PrevLogIndex + 1 + i
        if idx < len(rf.log) {
            // 如果该位置已有日志，检查 Term 是否冲突
            if rf.log[idx].Term != entry.Term {
                // 发现冲突！从这里开始截断，并准备追加后续所有日志
                rf.log = rf.log[:idx]
                appendStartIndex = i
                break // 退出循环，进行批量追加
            }
            // Term 相同，日志匹配，继续检查下一条
        } else {
            // 超出本地日志长度，说明从这里开始都是新的
            appendStartIndex = i
            break // 退出循环，进行批量追加
        }
    }

    // 如果发现了冲突点或新日志，进行追加
    if appendStartIndex != -1 {
        rf.log = append(rf.log, args.Entries[appendStartIndex:]...)
        logChanged = true
    }

    // 6. 更新 CommitIndex
	if args.LeaderCommit > rf.commitIndex {
		// 计算本次 RPC 确认的最新日志位置
		lastNewLogIndex := len(rf.log) - 1
		
		// 论文逻辑：commitIndex = min(LeaderCommit, Index of Last New Entry)
		var newCommitIndex int
		if args.LeaderCommit < lastNewLogIndex {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = lastNewLogIndex
		}

		// 【关键修正】只有当计算出的 newCommitIndex 比当前大时，才更新！
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			// fmt.Printf("Server %d updated commitIndex to %d\n", rf.me, rf.commitIndex)
		}
	}

    // 7. 持久化与返回
    if persistNeeded || logChanged {
        rf.persist()
    }

    reply.Success = true
    reply.Term = rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == stateLeader)

	if !isLeader {
		return index, term, isLeader
	}

	// append command to local log
	newLogEntry := LogEntry{
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	index = newLogEntry.Index
	rf.persist()
	if DLog != nil { DLog.Printf("[Log] Server %d Start command index %d term %d\n", rf.me, index, term) }

	// signal replicators to send new log entries
	rf.sendAllNewLogEntries()
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
	if DLog != nil { DLog.Printf("[Lifecycle] Server %d killed\n", rf.me) }
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// Placeholder for any election initialization logic
	rf.mu.Lock()
	lastLogIndex := len(rf.log)-1
	lastLogTerm := rf.log[lastLogIndex].Term

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = stateCandidate
	rf.resetElectionTimer()
	voteCount := 1 // vote for self
	if DLog != nil { DLog.Printf("[Role] Server %d starting election for term %d\n", rf.me, rf.currentTerm) }

	// send RequestVote RPCs to all other servers
	requestVoteArgs := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	rf.persist()
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
					rf.resetElectionTimer()
					rf.persist()
					if DLog != nil { DLog.Printf("[Role] Server %d became follower (higher term in RequestVote reply from %d)\n", rf.me, server) }
					return
				}

				if rf.state != stateCandidate || rf.currentTerm != requestVoteArgs.Term{
					// no longer candidate or term changed
					return
				}

				if requestVoteReply.VoteGranted {
					// count votes
					voteCount++
					if DLog != nil { DLog.Printf("[Vote] Server %d received vote from %d for term %d (total votes: %d)\n", rf.me, server, rf.currentTerm, voteCount) }
					// if votes received from majority of servers: become leader
					if voteCount > len(rf.peers)/2 {
						rf.state = stateLeader
						if DLog != nil { DLog.Printf("[Role] Server %d became leader for term %d\n", rf.me, rf.currentTerm) }
						// initialize leader state
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						nextLogIndex := len(rf.log)
						for i := range rf.peers {
							rf.nextIndex[i] = nextLogIndex
							rf.matchIndex[i] = 0
						}
						// send initial empty AppendEntries RPCs (heartbeats) to each server
						// go rf.broadcastHeartbeat()
						go rf.sendAllNewLogEntries()
					}
					return
				}
			}
		}(peer)

	}	
}


func (rf *Raft) sendAllNewLogEntries() {
	for peer := range rf.peers {
		if peer != rf.me {
			// signal replicator to send new log entries
			select {
			case rf.replicatorCond[peer] <- struct{}{}:
			default:
			}		
		}
	}	
}
func (rf *Raft) sendNewLogEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply)  bool {
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = stateFollower
			rf.votedFor = -1
			rf.resetElectionTimer()
			rf.persist()
			if DLog != nil { DLog.Printf("[Role] Server %d became follower (higher term in AppendEntries reply from %d)\n", rf.me, server) }
			return true
		}

		if rf.state != stateLeader || rf.currentTerm != args.Term {
			// no longer leader or term changed
			return true
		}
		// isLeader and term is correct
		if !reply.Success {
			// decrement nextIndex and retry
			xTerm := reply.XTerm
			xIndex := reply.XIndex
			xLen := reply.XLen
			
			if DLog != nil { DLog.Printf("[Log] Server %d AppendEntries failed for %d. XTerm %d, XIndex %d, XLen %d\n", rf.me, server, xTerm, xIndex, xLen) }

			if xTerm != 0 {
				// find xTerm in log
				termIndex := -1
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == xTerm {
						termIndex = i
						break
					}
				}
				if termIndex != -1 {
					// found xTerm
					rf.nextIndex[server] = termIndex + 1
				}else{
					// didn't find xTerm
					rf.nextIndex[server] = xIndex
				}
			}else{
				// follower's log is shorter than prevLogIndex
				rf.nextIndex[server] = xLen
			}

			if rf.nextIndex[server] < 1 {
        		rf.nextIndex[server] = 1
    		}	
			// rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
			// select {
			// case rf.replicatorCond[server] <- struct{}{}:
			// default:
			// }	
			return false
		}else{
			// update nextIndex and matchIndex
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// update commitIndex if possible
			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				count := 1 // count self
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				// 注意不能提交前朝的日志
				if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
				}
			}
		}			
	} else {
		if DLog != nil { DLog.Printf("[Comm] Server %d lost connection with %d (AppendEntries timeout)\n", rf.me, server) }
	}
	return ok
}

// func (rf *Raft) broadcastHeartbeat() {
//     rf.mu.Lock()
//     if rf.state != stateLeader {
//         rf.mu.Unlock()
//         return
//     }
    
//     // 1. 准备参数（在锁内一次性完成）
//     args := &AppendEntriesArgs{
//         Term:         rf.currentTerm,
//         LeaderId:     rf.me,
//         PrevLogIndex: len(rf.log) - 1,
//         PrevLogTerm:  rf.log[len(rf.log)-1].Term,
//         Entries:      nil, // 心跳为空
//         LeaderCommit: rf.commitIndex,
//     }
//     rf.mu.Unlock() // 构造完立刻解锁

//     // 2. 并行发送
//     for peer := range rf.peers {
//         if peer == rf.me { continue }
        
//         go func(server int) {
//             reply := &AppendEntriesReply{}
//             if rf.sendAppendEntries(server, args, reply) {
//                 rf.mu.Lock()
//                 defer rf.mu.Unlock()
                
//                 // 处理回复
//                 if reply.Term > rf.currentTerm {
//                     rf.currentTerm = reply.Term
//                     rf.state = stateFollower
//                     rf.votedFor = -1
//                 }
//             }
//         }(peer)
//     }
// }

func (rf *Raft) ticker() {
    for rf.killed() == false {
        time.Sleep(10 * time.Millisecond)

        rf.mu.Lock()
		state := rf.state
        lastRPC := rf.lastRPCtime
		timeout := rf.electionTimeout
        rf.mu.Unlock()


		// 使用保存的 timeout
		if state != stateLeader && time.Since(lastRPC) > timeout {
			rf.startElection()
		}
	
    }
}

func (rf *Raft) committer() {
    for rf.killed() == false {
        rf.mu.Lock()
        applyMsgs := []raftapi.ApplyMsg{}

        if rf.commitIndex > rf.lastApplied {
            for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
                msg := raftapi.ApplyMsg{
                    CommandValid: true,
                    Command:      rf.log[i].Command,
                    CommandIndex: rf.log[i].Index,
                }
                applyMsgs = append(applyMsgs, msg)
            }
            rf.lastApplied = rf.commitIndex
        }
        rf.mu.Unlock() // <--- 先解锁！

        // 在锁外发送
        for _, msg := range applyMsgs {
            rf.applyCh <- msg
        }

        time.Sleep(10 * time.Millisecond)
    }
}

// func (rf *Raft) replicator(server int) {
// 	heartbeatInterval := 50 * time.Millisecond
// 	for rf.killed() == false {
// 		// wait for signal to replicate
// 		cond := rf.replicatorCond[server]

// 		select{
// 			case <-cond:
// 			case <-time.After(heartbeatInterval):
// 		}
// 		rf.mu.Lock()
// 		if rf.state != stateLeader {
// 			rf.mu.Unlock()
// 			continue
// 		}

// 		// send log entries to follower
// 		serverNextIndex := rf.nextIndex[server]
// 		prevLogIndex := serverNextIndex - 1
// 		prevLogTerm := rf.log[prevLogIndex].Term
// 		entries := make([]LogEntry, len(rf.log[serverNextIndex:]))
// 		copy(entries, rf.log[serverNextIndex:])

// 		args := &AppendEntriesArgs{
// 			Term:         rf.currentTerm,
// 			LeaderId:     rf.me,
// 			PrevLogIndex: prevLogIndex,
// 			PrevLogTerm:  prevLogTerm,
// 			Entries:      entries,
// 			LeaderCommit: rf.commitIndex,
// 		}
// 		rf.mu.Unlock()

// 		reply := &AppendEntriesReply{}

// 		rf.sendNewLogEntries(server, args, reply)
// 	}	
// }


func (rf *Raft) replicator(server int) {
    heartbeatInterval := 50 * time.Millisecond
    shouldRetry := false
    for rf.killed() == false {
        // Wait for signal or timeout
        if !shouldRetry {
            cond := rf.replicatorCond[server]
            select {
            case <-cond:
            case <-time.After(heartbeatInterval):
            }
        }
        shouldRetry = false

        rf.mu.Lock()
        if rf.state != stateLeader {
            rf.mu.Unlock()
            continue
        }

		if DLog != nil { DLog.Printf("[Replicator] Server %d replicating to %d. nextIndex %d, logLen %d\n", rf.me, server, rf.nextIndex[server], len(rf.log)) }

        
        serverNextIndex := rf.nextIndex[server]
        prevLogIndex := serverNextIndex - 1
        
        
        prevLogTerm := rf.log[prevLogIndex].Term
        
        // 即使日志很长，一次也只发 100 条
        // 这对于通过 Unreliable 测试至关重要！
        batchSize := 100 
        lastIndex := len(rf.log) - 1
        var entries []LogEntry
        
        if serverNextIndex <= lastIndex {
            endIndex := serverNextIndex + batchSize
            if endIndex > len(rf.log) {
                endIndex = len(rf.log)
            }
            // 只有当有数据发送时才分配内存
            entries = make([]LogEntry, endIndex-serverNextIndex)
            copy(entries, rf.log[serverNextIndex:endIndex])
        }

        args := &AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderId:     rf.me,
            PrevLogIndex: prevLogIndex,
            PrevLogTerm:  prevLogTerm,
            Entries:      entries,
            LeaderCommit: rf.commitIndex,
        }
        rf.mu.Unlock()
		go func(){
			reply := &AppendEntriesReply{}
			if !rf.sendNewLogEntries(server, args, reply) {
				shouldRetry = true
				time.Sleep(20 * time.Millisecond) // 避免忙等待
			}else {
				// Case B: 发送成功。
				// 检查是否还有更多日志需要发送？如果有，不要等待，立即触发下一次发送
				rf.mu.Lock()
				if rf.nextIndex[server] < len(rf.log) {
					shouldRetry = true // 还有数据，趁热打铁，立即发送下一批
				}
				rf.mu.Unlock()
				time.Sleep(20 * time.Millisecond) // 避免忙等待
			}
		}()


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
	rf.applyCh = applyCh
	rf.replicatorCond = make([]chan struct{}, len(peers))
	for i := range rf.replicatorCond {
		rf.replicatorCond[i] = make(chan struct{}, 1)
		if i != me{
			go rf.replicator(i)
		}
	}
	// Your initialization code here (3A, 3B, 3C).
	source := rand.NewSource(time.Now().UnixNano() + int64(me))
	rf.rng = rand.New(source)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil}) // dummy log entry at index 0
	rf.state = stateFollower
	rf.resetElectionTimer()
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if DLog != nil { DLog.Printf("[Lifecycle] Server %d started\n", rf.me) }

	// start ticker goroutine to start elections
	go rf.ticker()

	// send commit messages to applyCh
	go rf.committer()

	return rf
}
