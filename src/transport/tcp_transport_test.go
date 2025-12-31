package transport

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ==================== 测试用的 RPC 服务 ====================

// TestService 模拟 Raft 的 RPC 服务
type TestService struct {
	callCount int
}

// RequestVote 模拟 Raft 的 RequestVote RPC
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (s *TestService) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	s.callCount++
	reply.Term = args.Term
	reply.VoteGranted = true
}

// AppendEntries 模拟 Raft 的 AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (s *TestService) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	s.callCount++
	reply.Term = args.Term
	reply.Success = len(args.Entries) >= 0
}

// Echo 简单的 echo 服务
type EchoArgs struct {
	Message string
}

type EchoReply struct {
	Message string
}

func (s *TestService) Echo(args EchoArgs, reply *EchoReply) {
	s.callCount++
	reply.Message = "Echo: " + args.Message
}

// ==================== 测试用例 ====================

// TestBasicRPC 测试基本的 RPC 调用
func TestBasicRPC(t *testing.T) {
	// 1. 创建服务端
	serverTransport := NewTCPTransport()
	service := &TestService{}
	if err := serverTransport.Register(service); err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	if err := serverTransport.Listen(":19000"); err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer serverTransport.Close()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 2. 创建客户端
	clientTransport := NewTCPTransport()
	defer clientTransport.Close()
	client := clientTransport.MakeClient("localhost:19000")

	// 3. 测试 RequestVote
	args := RequestVoteArgs{Term: 1, CandidateId: 0}
	reply := RequestVoteReply{}
	ok := client.Call("TestService.RequestVote", args, &reply)
	if !ok {
		t.Fatal("RequestVote RPC failed")
	}
	if reply.Term != 1 || !reply.VoteGranted {
		t.Fatalf("Unexpected reply: %+v", reply)
	}

	// 4. 测试 Echo
	echoArgs := EchoArgs{Message: "Hello"}
	echoReply := EchoReply{}
	ok = client.Call("TestService.Echo", echoArgs, &echoReply)
	if !ok {
		t.Fatal("Echo RPC failed")
	}
	if echoReply.Message != "Echo: Hello" {
		t.Fatalf("Unexpected echo reply: %s", echoReply.Message)
	}

	t.Logf("Service call count: %d", service.callCount)
}

// TestAppendEntries 测试 AppendEntries RPC
func TestAppendEntries(t *testing.T) {
	// 1. 创建服务端
	serverTransport := NewTCPTransport()
	service := &TestService{}
	serverTransport.Register(service)
	serverTransport.Listen(":19001")
	defer serverTransport.Close()

	time.Sleep(100 * time.Millisecond)

	// 2. 创建客户端
	clientTransport := NewTCPTransport()
	defer clientTransport.Close()
	client := clientTransport.MakeClient("localhost:19001")

	// 3. 测试 AppendEntries
	args := AppendEntriesArgs{
		Term:     2,
		LeaderId: 1,
		Entries:  []string{"cmd1", "cmd2", "cmd3"},
	}
	reply := AppendEntriesReply{}
	ok := client.Call("TestService.AppendEntries", args, &reply)
	if !ok {
		t.Fatal("AppendEntries RPC failed")
	}
	if reply.Term != 2 || !reply.Success {
		t.Fatalf("Unexpected reply: %+v", reply)
	}
}

// TestMultipleClients 测试多个客户端并发调用
func TestMultipleClients(t *testing.T) {
	// 1. 创建服务端
	serverTransport := NewTCPTransport()
	service := &TestService{}
	serverTransport.Register(service)
	serverTransport.Listen(":19002")
	defer serverTransport.Close()

	time.Sleep(100 * time.Millisecond)

	// 2. 并发创建多个客户端进行调用
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			clientTransport := NewTCPTransport()
			defer clientTransport.Close()
			client := clientTransport.MakeClient("localhost:19002")

			args := RequestVoteArgs{Term: id, CandidateId: id}
			reply := RequestVoteReply{}
			ok := client.Call("TestService.RequestVote", args, &reply)
			if !ok {
				t.Errorf("Client %d: RPC failed", id)
			}
			done <- true
		}(i)
	}

	// 等待所有客户端完成
	for i := 0; i < 10; i++ {
		<-done
	}

	t.Logf("All 10 concurrent clients completed. Service call count: %d", service.callCount)
}

// TestConnectionFailure 测试连接失败的情况
func TestConnectionFailure(t *testing.T) {
	clientTransport := NewTCPTransport()
	defer clientTransport.Close()

	// 尝试连接一个不存在的服务器
	client := clientTransport.MakeClient("localhost:19999")
	args := EchoArgs{Message: "test"}
	reply := EchoReply{}

	// 应该返回 false（模拟 labrpc 的行为）
	ok := client.Call("TestService.Echo", args, &reply)
	if ok {
		t.Fatal("Expected RPC to fail for non-existent server")
	}
	t.Log("Connection failure handled correctly")
}

// TestMultipleCalls 测试同一连接上的多次调用
func TestMultipleCalls(t *testing.T) {
	// 1. 创建服务端
	serverTransport := NewTCPTransport()
	service := &TestService{}
	serverTransport.Register(service)
	serverTransport.Listen(":19003")
	defer serverTransport.Close()

	time.Sleep(100 * time.Millisecond)

	// 2. 创建客户端并进行多次调用
	clientTransport := NewTCPTransport()
	defer clientTransport.Close()
	client := clientTransport.MakeClient("localhost:19003")

	for i := 0; i < 100; i++ {
		args := RequestVoteArgs{Term: i, CandidateId: i % 5}
		reply := RequestVoteReply{}
		ok := client.Call("TestService.RequestVote", args, &reply)
		if !ok {
			t.Fatalf("RPC %d failed", i)
		}
		if reply.Term != i {
			t.Fatalf("RPC %d: unexpected term %d", i, reply.Term)
		}
	}

	t.Logf("100 sequential calls completed. Service call count: %d", service.callCount)
}

// ==================== 性能基准测试 ====================

// BenchmarkPoolSize 测试不同连接池大小对并发性能的影响
func BenchmarkPoolSize(b *testing.B) {
	// 测试参数：不同的连接池大小 × 不同的并发数
	poolSizes := []int{1, 2, 5, 10}
	concurrencies := []int{1, 5, 10, 20, 50}

	for _, poolSize := range poolSizes {
		for _, concurrency := range concurrencies {
			name := fmt.Sprintf("pool=%d/conc=%d", poolSize, concurrency)
			b.Run(name, func(b *testing.B) {
				benchmarkWithConfig(b, poolSize, concurrency)
			})
		}
	}
}

// benchmarkWithConfig 使用指定配置运行基准测试
func benchmarkWithConfig(b *testing.B, poolSize, concurrency int) {
	// 创建服务端
	serverTransport := NewTCPTransport()
	service := &TestService{}
	serverTransport.Register(service)
	serverTransport.Listen(":0") // 使用随机端口
	defer serverTransport.Close()

	// 获取实际监听地址
	addr := serverTransport.listener.Addr().String()
	time.Sleep(50 * time.Millisecond)

	// 创建指定连接池大小的客户端
	clientTransport := NewTCPTransportWithPoolSize(poolSize)
	defer clientTransport.Close()
	client := clientTransport.MakeClient(addr)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrency)

		for j := 0; j < concurrency; j++ {
			go func() {
				defer wg.Done()
				args := RequestVoteArgs{Term: 1, CandidateId: 1}
				reply := RequestVoteReply{}
				client.Call("TestService.RequestVote", args, &reply)
			}()
		}

		wg.Wait()
	}
}

// TestPoolSizePerformance 用于直观展示不同配置的性能差异
func TestPoolSizePerformance(t *testing.T) {
	poolSizes := []int{1, 3, 5, 10}
	concurrency := 20
	iterations := 100

	t.Logf("\n性能测试: %d 并发请求 × %d 次迭代", concurrency, iterations)
	t.Logf("%-15s %-15s %-15s %-15s", "连接池大小", "总耗时", "平均延迟", "吞吐量(req/s)")
	t.Logf("%-15s %-15s %-15s %-15s", "----------", "------", "--------", "-----------")

	for _, poolSize := range poolSizes {
		// 创建服务端
		serverTransport := NewTCPTransport()
		service := &TestService{}
		serverTransport.Register(service)
		serverTransport.Listen(":0")
		addr := serverTransport.listener.Addr().String()
		time.Sleep(50 * time.Millisecond)

		// 创建客户端
		clientTransport := NewTCPTransportWithPoolSize(poolSize)
		client := clientTransport.MakeClient(addr)

		// 运行测试
		start := time.Now()
		for i := 0; i < iterations; i++ {
			var wg sync.WaitGroup
			wg.Add(concurrency)

			for j := 0; j < concurrency; j++ {
				go func() {
					defer wg.Done()
					args := RequestVoteArgs{Term: 1, CandidateId: 1}
					reply := RequestVoteReply{}
					client.Call("TestService.RequestVote", args, &reply)
				}()
			}
			wg.Wait()
		}
		elapsed := time.Since(start)

		// 计算指标
		totalRequests := concurrency * iterations
		avgLatency := elapsed / time.Duration(totalRequests)
		throughput := float64(totalRequests) / elapsed.Seconds()

		t.Logf("%-15d %-15s %-15s %-15.0f",
			poolSize,
			elapsed.Round(time.Millisecond),
			avgLatency.Round(time.Microsecond),
			throughput,
		)

		clientTransport.Close()
		serverTransport.Close()
	}
}
