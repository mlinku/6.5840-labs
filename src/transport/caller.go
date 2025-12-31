package transport

// Caller 是一个通用的 RPC 调用接口
// labrpc.ClientEnd 和 transport.ClientEnd 都隐式实现了这个接口
// 这样 Raft 代码可以无缝切换
type Caller interface {
	// Call 发送一个 RPC 请求并等待响应
	// svcMeth: 服务名.方法名，如 "Raft.RequestVote"
	// args: 请求参数
	// reply: 响应对象的指针
	// 返回值: true 表示成功，false 表示失败
	Call(svcMeth string, args interface{}, reply interface{}) bool
}
