package transport

//
// TCP Transport 实现
// 用于替换 labrpc 的基于 channel 的模拟网络，支持真正的跨进程/跨机器通信
//
// 使用方法：
//   transport := NewTCPTransport()
//   transport.Register(raftInstance)           // 注册 RPC 服务
//   transport.Listen(":9000")                  // 开始监听
//   client := transport.MakeClient("host:9000") // 创建客户端
//   client.Call("Raft.RequestVote", &args, &reply)
//

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

// ==================== 接口定义 ====================

// Transport 定义了 RPC 传输层的接口
// 未来可以实现 TCPTransport、RDMATransport 等
type Transport interface {
	// Register 注册一个 RPC 服务对象
	// rcvr 的导出方法将作为 RPC 处理函数
	Register(rcvr interface{}) error

	// Listen 开始监听指定地址
	Listen(addr string) error

	// MakeClient 创建一个到指定地址的客户端
	MakeClient(addr string) *ClientEnd

	// Close 关闭传输层
	Close()
}

// ==================== RPC 消息结构 ====================

// RPCRequest 表示一个 RPC 请求
type RPCRequest struct {
	ServiceMethod string // 格式: "Service.Method"，如 "Raft.RequestVote"
	Args          []byte // gob 编码后的参数
}

// RPCResponse 表示一个 RPC 响应
type RPCResponse struct {
	OK    bool   // 是否成功
	Reply []byte // gob 编码后的响应
	Error string // 错误信息（如果有）
}

// ==================== ClientEnd 客户端端点 ====================

// ClientEnd 代表一个 RPC 客户端端点
// 【重要】这个结构体的 Call 方法签名与 labrpc.ClientEnd 完全兼容
// 所以 Raft 代码可以无缝切换
type ClientEnd struct {
	addr      string        // 目标服务器地址
	transport *TCPTransport // 所属的 Transport（用于获取连接）
	timeout   time.Duration // RPC 超时时间
}

// Call 发送一个 RPC 请求并等待响应
// 【重要】签名与 labrpc.ClientEnd.Call 完全相同
// svcMeth: 服务名.方法名，如 "Raft.RequestVote"
// args: 请求参数（会被 gob 编码）
// reply: 响应对象的指针（会被 gob 解码填充）
// 返回值: true 表示成功，false 表示失败（网络错误、超时等）
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	// 1. 序列化参数
	var argsBuf bytes.Buffer
	enc := gob.NewEncoder(&argsBuf)
	if err := enc.Encode(args); err != nil {
		log.Printf("[TCPTransport] Failed to encode args: %v", err)
		return false
	}

	// 2. 构建请求
	req := RPCRequest{
		ServiceMethod: svcMeth,
		Args:          argsBuf.Bytes(),
	}

	// 3. 发送请求并获取响应
	resp, err := e.transport.sendRequest(e.addr, &req, e.timeout)
	if err != nil {
		// 网络错误，静默失败（模拟 labrpc 的行为）
		return false
	}

	// 4. 检查响应
	if !resp.OK {
		return false
	}

	// 5. 反序列化响应
	replyBuf := bytes.NewBuffer(resp.Reply)
	dec := gob.NewDecoder(replyBuf)
	if err := dec.Decode(reply); err != nil {
		log.Printf("[TCPTransport] Failed to decode reply: %v", err)
		return false
	}

	return true
}

// ==================== TCPTransport 实现 ====================

// TCPTransport 基于 TCP 的 RPC 传输层
type TCPTransport struct {
	mu       sync.RWMutex
	addr     string       // 监听地址
	listener net.Listener // TCP 监听器
	services *ServiceMap  // 注册的 RPC 服务
	clients  *ClientPool  // 连接池
	closed   bool         // 是否已关闭
	done     chan struct{}
}

// NewTCPTransport 创建一个新的 TCP 传输层
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		services: NewServiceMap(),
		clients:  NewClientPool(),
		done:     make(chan struct{}),
	}
}

// NewTCPTransportWithPoolSize 创建指定连接池大小的 TCP 传输层
func NewTCPTransportWithPoolSize(maxConns int) *TCPTransport {
	return &TCPTransport{
		services: NewServiceMap(),
		clients:  NewClientPoolWithSize(maxConns),
		done:     make(chan struct{}),
	}
}

// Register 注册一个 RPC 服务
// rcvr 是一个结构体指针，其导出方法将作为 RPC 处理函数
// 方法签名要求：func (t *T) MethodName(args *Args, reply *Reply)
func (t *TCPTransport) Register(rcvr interface{}) error {
	return t.services.Register(rcvr)
}

// Listen 开始监听指定地址
func (t *TCPTransport) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	t.mu.Lock()
	t.addr = addr
	t.listener = listener
	t.mu.Unlock()

	// 启动接受连接的 goroutine
	go t.acceptLoop()

	log.Printf("[TCPTransport] Listening on %s", addr)
	return nil
}

// MakeClient 创建一个到指定地址的客户端端点
func (t *TCPTransport) MakeClient(addr string) *ClientEnd {
	return &ClientEnd{
		addr:      addr,
		transport: t,
		timeout:   10 * time.Second, // 默认 10 秒超时
	}
}

// Close 关闭传输层
func (t *TCPTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}
	t.closed = true
	close(t.done)

	if t.listener != nil {
		t.listener.Close()
	}

	t.clients.CloseAll()
	log.Printf("[TCPTransport] Closed")
}

// acceptLoop 接受新连接的循环
func (t *TCPTransport) acceptLoop() {
	for {
		select {
		case <-t.done:
			return
		default:
		}

		conn, err := t.listener.Accept()
		if err != nil {
			// 检查是否是正常关闭
			select {
			case <-t.done:
				return
			default:
				log.Printf("[TCPTransport] Accept error: %v", err)
				continue
			}
		}

		// 为每个连接启动一个 handler goroutine
		go t.handleConnection(conn)
	}
}

// handleConnection 处理一个客户端连接
func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		select {
		case <-t.done:
			return
		default:
		}

		// 1. 读取请求
		req, err := readRequest(reader)
		if err != nil {
			if err != io.EOF {
				// log.Printf("[TCPTransport] Read request error: %v", err)
			}
			return
		}

		// 2. 处理请求
		resp := t.handleRequest(req)

		// 3. 发送响应
		if err := writeResponse(writer, resp); err != nil {
			log.Printf("[TCPTransport] Write response error: %v", err)
			return
		}
	}
}

// handleRequest 处理一个 RPC 请求
func (t *TCPTransport) handleRequest(req *RPCRequest) *RPCResponse {
	// 1. 解析服务名和方法名
	dot := strings.LastIndex(req.ServiceMethod, ".")
	if dot < 0 {
		return &RPCResponse{OK: false, Error: "invalid service method: " + req.ServiceMethod}
	}
	serviceName := req.ServiceMethod[:dot]
	methodName := req.ServiceMethod[dot+1:]

	// 2. 查找服务和方法
	svc, method, err := t.services.Lookup(serviceName, methodName)
	if err != nil {
		return &RPCResponse{OK: false, Error: err.Error()}
	}

	// 3. 准备参数
	// 获取方法的参数类型（第 1 个参数是 args）
	argType := method.Type.In(1)
	argIsPtr := argType.Kind() == reflect.Ptr

	// 始终创建一个指针用于解码
	var argvPtr reflect.Value
	if argIsPtr {
		argvPtr = reflect.New(argType.Elem()) // *T -> 创建 *T
	} else {
		argvPtr = reflect.New(argType) // T -> 创建 *T
	}

	// 反序列化参数（gob.Decode 需要指针）
	argsBuf := bytes.NewBuffer(req.Args)
	dec := gob.NewDecoder(argsBuf)
	if err := dec.Decode(argvPtr.Interface()); err != nil {
		return &RPCResponse{OK: false, Error: "decode args error: " + err.Error()}
	}

	// 4. 准备 reply
	replyType := method.Type.In(2)
	replyv := reflect.New(replyType.Elem()) // reply 总是 *T

	// 5. 调用方法
	// 根据方法签名传递正确的参数类型
	function := method.Func
	var argv reflect.Value
	if argIsPtr {
		argv = argvPtr // 方法期望 *T，传递 *T
	} else {
		argv = argvPtr.Elem() // 方法期望 T，传递 T
	}
	function.Call([]reflect.Value{svc.rcvr, argv, replyv})

	// 6. 序列化响应
	var replyBuf bytes.Buffer
	enc := gob.NewEncoder(&replyBuf)
	if err := enc.Encode(replyv.Interface()); err != nil {
		return &RPCResponse{OK: false, Error: "encode reply error: " + err.Error()}
	}

	return &RPCResponse{OK: true, Reply: replyBuf.Bytes()}
}

// sendRequest 发送请求并等待响应
func (t *TCPTransport) sendRequest(addr string, req *RPCRequest, timeout time.Duration) (*RPCResponse, error) {
	// 1. 获取或创建连接
	conn, err := t.clients.Get(addr, timeout)
	if err != nil {
		return nil, err
	}

	// 2. 设置超时
	conn.SetDeadline(time.Now().Add(timeout))

	// 3. 发送请求
	writer := bufio.NewWriter(conn)
	if err := writeRequest(writer, req); err != nil {
		t.clients.Remove(addr) // 连接可能坏了，移除
		return nil, err
	}

	// 4. 读取响应
	reader := bufio.NewReader(conn)
	resp, err := readResponse(reader)
	if err != nil {
		t.clients.Remove(addr) // 连接可能坏了，移除
		return nil, err
	}

	// 5. 归还连接
	t.clients.Put(addr, conn)

	return resp, nil
}

// ==================== 服务注册表 ====================

// Service 表示一个已注册的 RPC 服务
type Service struct {
	name    string                    // 服务名称
	rcvr    reflect.Value             // 接收者对象
	typ     reflect.Type              // 接收者类型
	methods map[string]reflect.Method // 方法映射
}

// ServiceMap 管理所有注册的服务
type ServiceMap struct {
	mu       sync.RWMutex
	services map[string]*Service
}

// NewServiceMap 创建服务注册表
func NewServiceMap() *ServiceMap {
	return &ServiceMap{
		services: make(map[string]*Service),
	}
}

// Register 注册一个服务
func (sm *ServiceMap) Register(rcvr interface{}) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = make(map[string]reflect.Method)

	// 遍历所有方法，筛选符合 RPC 签名的方法
	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		// 检查方法签名：
		// - 必须是导出方法（PkgPath == ""）
		// - 必须有 3 个输入参数（receiver, args, *reply）
		// - reply 必须是指针
		// - 无返回值 或 返回值数量为 0
		if method.PkgPath != "" {
			continue // 非导出方法
		}
		if mtype.NumIn() != 3 {
			continue // 参数数量不对
		}
		if mtype.In(2).Kind() != reflect.Ptr {
			continue // reply 必须是指针
		}
		if mtype.NumOut() != 0 {
			continue // 不能有返回值
		}

		svc.methods[mname] = method
	}

	if len(svc.methods) == 0 {
		return fmt.Errorf("no valid RPC methods found in %s", svc.name)
	}

	sm.services[svc.name] = svc
	log.Printf("[TCPTransport] Registered service: %s with %d methods", svc.name, len(svc.methods))
	return nil
}

// Lookup 查找服务和方法
func (sm *ServiceMap) Lookup(serviceName, methodName string) (*Service, reflect.Method, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	svc, ok := sm.services[serviceName]
	if !ok {
		return nil, reflect.Method{}, fmt.Errorf("unknown service: %s", serviceName)
	}

	method, ok := svc.methods[methodName]
	if !ok {
		return nil, reflect.Method{}, fmt.Errorf("unknown method: %s.%s", serviceName, methodName)
	}

	return svc, method, nil
}

// ==================== 连接池 ====================

// ClientPool 管理客户端连接（多连接池，每个地址可保持多个连接）
type ClientPool struct {
	mu       sync.Mutex
	conns    map[string][]net.Conn // 每个地址一个连接列表
	maxConns int                    // 每地址最大连接数
}

// NewClientPool 创建连接池
func NewClientPool() *ClientPool {
	return &ClientPool{
		conns:    make(map[string][]net.Conn),
		maxConns: 5, // 默认每地址最多 5 个连接
	}
}

// NewClientPoolWithSize 创建指定大小的连接池
func NewClientPoolWithSize(maxConns int) *ClientPool {
	return &ClientPool{
		conns:    make(map[string][]net.Conn),
		maxConns: maxConns,
	}
}

// SetMaxConns 设置每地址最大连接数
func (p *ClientPool) SetMaxConns(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxConns = n
}

// Get 获取或创建到指定地址的连接
func (p *ClientPool) Get(addr string, timeout time.Duration) (net.Conn, error) {
	p.mu.Lock()
	if list := p.conns[addr]; len(list) > 0 {
		// 从列表尾部取出一个连接
		conn := list[len(list)-1]
		p.conns[addr] = list[:len(list)-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	// 池中没有可用连接，创建新连接
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Put 归还连接到池中
func (p *ClientPool) Put(addr string, conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	list := p.conns[addr]
	if len(list) >= p.maxConns {
		// 超过最大连接数，关闭不复用
		conn.Close()
		return
	}
	p.conns[addr] = append(list, conn)
}

// Remove 移除并关闭指定地址的所有连接
func (p *ClientPool) Remove(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if list, ok := p.conns[addr]; ok {
		for _, conn := range list {
			conn.Close()
		}
		delete(p.conns, addr)
	}
}

// CloseAll 关闭所有连接
func (p *ClientPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, list := range p.conns {
		for _, conn := range list {
			conn.Close()
		}
	}
	p.conns = make(map[string][]net.Conn)
}

// ==================== 消息编解码 ====================

// writeRequest 写入请求到连接
func writeRequest(w *bufio.Writer, req *RPCRequest) error {
	enc := gob.NewEncoder(w)
	if err := enc.Encode(req); err != nil {
		return err
	}
	return w.Flush()
}

// readRequest 从连接读取请求
func readRequest(r *bufio.Reader) (*RPCRequest, error) {
	dec := gob.NewDecoder(r)
	req := &RPCRequest{}
	if err := dec.Decode(req); err != nil {
		return nil, err
	}
	return req, nil
}

// writeResponse 写入响应到连接
func writeResponse(w *bufio.Writer, resp *RPCResponse) error {
	enc := gob.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		return err
	}
	return w.Flush()
}

// readResponse 从连接读取响应
func readResponse(r *bufio.Reader) (*RPCResponse, error) {
	dec := gob.NewDecoder(r)
	resp := &RPCResponse{}
	if err := dec.Decode(resp); err != nil {
		return nil, err
	}
	return resp, nil
}
