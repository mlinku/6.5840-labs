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

#### task2 √
- 在Clerk.Put 和 Clerk.Get 调用基础上构建锁，支持`Acquire` 和 `Release`
- 锁的规范是：一次只能有一个客户端成功获取锁；其他客户端必须等待，直到第一个客户端使用 Release 释放锁。
- `Acquire` 和 `Release` 代码可以通过调用 lk.ck.Put() 和 lk.ck.Get() 与您的键/值服务器通信
- **要注意put的结果是否正确，防止两个同时请求**
- **sleep防止一直重发**

#### task3
- 网络不稳定时，RPC请求可能乱序延迟，**Clerk会重试RPC**,直到服务器回复。
- 重发GET报文是安全的，重发PUT如果版本号相同则安全，若不同则会回复rpc.ErrVersion
- 如果 Clerk 收到`重传`的 Put RPC 的 rpc.ErrVersion ， Clerk.Put 必须向应用程序返回 rpc.ErrMaybe 而不是 rpc.ErrVersion ，因为请求可能已被执行。然后由应用程序来处理这种情况。
- 修改`kvsrv1/client.go`，使其能够在 RPC 请求和回复被丢弃的情况下继续运行
   - 客户端的 ck.clnt.Call() 返回 true 表示客户端收到了服务器的 RPC 回复；返回 false 表示没有收到回复
   - 你的解决方案不应该需要对服务器进行任何更改
- **调用RPC时，reply要是全新的**