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

#### task4
- 存在errmaybe被锁的情况，但是不知道是自己的还是别人的，导致出错，因此要定义ID

MIT 6.5840 Lab 1 (Lock) 错误复盘简报

1. 基础机制层面

RPC 变量污染

现象：测试报错 labgob warning: Decoding into a non-default variable。

原因：在循环外定义 reply 变量，导致上一次调用的残留数据影响下一次解码。

✅ 修正：将 reply 声明移至 for 循环内部，确保每次调用前变量为零值。

死循环 (Hang)

现象：测试超时。

原因：Release 在收到 ErrVersion（意味着锁已释放或版本已更新）时，逻辑判断错误导致无限重试。

✅ 修正：Release 遇到 ErrVersion 应直接视为操作成功（锁已不在我手中）并返回。

2. 并发控制层面

破坏原子性 (Race Condition)

现象：多个客户端通过 Check-Then-Act 逻辑同时认为自己可以加锁。

原因：Acquire 中先 Get 检查状态，再直接 Put，中间存在时间隙。

✅ 修正：利用 Put 的 version 参数实现 CAS (Compare-And-Swap) 机制，只有版本匹配才允许写入。

3. 不可靠网络层面 (核心难点)

自我死锁 (Self-Deadlock)

现象：加锁成功但 ACK 丢失，客户端重试后发现锁被占用，误以为是别人抢占，于是休眠等待自己释放锁。

身份冒领 (Critical Bug)

现象：测试报错 two clients acquired lock。

原因：使用通用的 "locked" 字符串作为锁的值。Client B 在重试时，误将 Client A 成功的锁（状态 "locked"）当成是自己丢包后的成功结果。

✅ 修正：引入 Unique ID（随机数或 UUID），只有 Get 回来的值严格等于自己的 ID 时