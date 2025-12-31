package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"6.5840/transport"
)

func init() {
	// 注册 Raft RPC 类型到 gob（必须在使用前注册）
	gob.Register(raft.RequestVoteArgs{})
	gob.Register(raft.RequestVoteReply{})
	gob.Register(raft.AppendEntriesArgs{})
	gob.Register(raft.AppendEntriesReply{})
	gob.Register(raft.InstallSnapshotArgs{})
	gob.Register(raft.InstallSnapshotReply{})
	gob.Register(raft.LogEntry{})
}

func main() {
	// 命令行参数
	id := flag.Int("id", 0, "node id (0, 1, 2, ...)")
	addr := flag.String("addr", ":9000", "listen address")
	peersStr := flag.String("peers", "", "comma-separated peer addresses (e.g., localhost:9000,localhost:9001,localhost:9002)")
	flag.Parse()

	if *peersStr == "" {
		log.Fatal("--peers is required")
	}

	peerAddrs := strings.Split(*peersStr, ",")
	if *id < 0 || *id >= len(peerAddrs) {
		log.Fatalf("invalid id %d for %d peers", *id, len(peerAddrs))
	}

	log.Printf("Starting Raft node %d at %s", *id, *addr)
	log.Printf("Peers: %v", peerAddrs)

	// 创建 Transport
	tp := transport.NewTCPTransport()

	// 创建到每个 peer 的客户端
	peers := make([]transport.Caller, len(peerAddrs))
	for i, peerAddr := range peerAddrs {
		peers[i] = tp.MakeClient(peerAddr)
	}

	// 创建 Persister（内存版本，重启会丢失状态）
	persister := tester.MakePersister()

	// 创建 ApplyMsg channel
	applyCh := make(chan raftapi.ApplyMsg, 100)

	// 启动 apply 消息处理 goroutine
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				log.Printf("[Node %d] Applied command at index %d: %v", *id, msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				log.Printf("[Node %d] Applied snapshot up to index %d", *id, msg.SnapshotIndex)
			}
		}
	}()

	// 创建 Raft 实例
	rf := raft.MakeWithCallers(peers, *id, persister, applyCh)

	// 将 Raft 实例包装并注册到 Transport
	// 注意：Raft 实现了 raftapi.Raft 接口，但 RPC 方法在具体实现上
	// 我们需要获取底层的 *raft.Raft 来注册
	raftImpl := rf.(*raft.Raft)
	if err := tp.Register(raftImpl); err != nil {
		log.Fatalf("Failed to register Raft RPC: %v", err)
	}

	// 开始监听
	if err := tp.Listen(*addr); err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}

	log.Printf("[Node %d] Raft node started successfully", *id)

	// 定期打印状态
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			term, isLeader := rf.GetState()
			role := "Follower"
			if isLeader {
				role = "Leader"
			}
			log.Printf("[Node %d] Term: %d, Role: %s", *id, term, role)
		}
	}()

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	rf.Kill()
	tp.Close()
}
