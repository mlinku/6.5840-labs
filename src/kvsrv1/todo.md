## total task
- 每个客户端使用 Clerk 与键/值服务器交互，Clerk 向服务器发送 RPC。
- 客户端可以向服务器发送两种不同的 RPC： Put(key, value, version) 和 Get(key) 。
- 服务器维护一个内存映射，记录每个键对应的(值, 版本)元组。键和值都是字符串。版本号记录了该键被写入的次数。
 - 改变键：Put(key, value, version) 在版本号与服务器中该键的版本号匹配时，进行改变。
    - 如果版本号匹配，服务器还会递增该键的版本号。
    - 如果版本号不匹配，服务器应返回 rpc.ErrVersion 。
 - 创建键：客户端调用 Put 并将版本号设为 0 来创建新键（服务器存储的最终版本将为 1）。
    - 如果 Put 的版本号大于 0 且键不存在，服务器应返回 rpc.ErrNoKey 
 - 读取键：Get(key) 获取键的当前值及其关联版本。如果键在服务器上不存在，服务器应返回 rpc.ErrNoKey 。
#### note
- kvsrv1/client.go 实现了一个 Clerk，客户端用它来管理与服务器的 RPC 交互；
 - Clerk 提供了 Put 和 Get 方法。 
- kvsrv1/server.go 包含服务器代码，包括实现 RPC 请求服务器端的 Put 和 Get 处理器。
- 仅需要修改 client.go 和 server.go 。
- RPC 请求、回复和错误值在 kvsrv1/rpc/rpc.go 文件中的kvsrv1/rpc 包中定义，不必修改 。