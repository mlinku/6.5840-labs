### total task
- 基于实验3的raft库，构建容错的kv存储服务。
- 服务是由一组服务器组成，利用raft来维护一致的数据库
    - 只要多数服务器存活，即可继续处理客户端请求。
- 所有服务器必须为并发请求选择相同的执行顺序
    - 避免过时状态回复
    - 在故障后恢复状态
    - 保留所有已确认的客户端更新
- 实现
    - rsm中定义的tateMachine 接口，以便使用 rsm 进行与服务器无关的自我复制
    - 修改 kvraft1/client.go 和 kvraft1/server.go 来实现服务器特定的部分

#### task 4A
- 服务通过两种方式与raft交互
    - 服务领导者调用raft.Start() 提交客户端操作
    - 所有服务副本则通过 Raft 的 applyCh 接收已提交的操作并执行
        - 每个服务执行applyCh时，需要将执行结果传递给调用raft.Start的协程，以便返回给客户端
- 由rsm封装上述交互过程，作为服务与Raft之间的中间层。
    - 在rsm.go中实现
        - 一个读取applyCh的读取器协程
        - 一个rsm.Submit()函数，为客户端操作调用raft.Start()操作，然后等待读取器协程传递该操作的执行结果。
- 服务应将每个客户端操作传递给rsm.Submit()，其将每个客户端操作封装在Op结构并附带唯一标识符。
- 流程：
    - 客户端向服务领导者发送请求。
    - 服务领导者使用该请求调用 rsm.Submit() 。
    - rsm.Submit() 调用 raft.Start() 并传递请求，然后进入等待状态。
    - Raft 提交该请求并将其发送至所有对等节点的 applyCh 。
    - 每个对等节点上的 rsm 读取协程从 applyCh 中读取请求，并将其传递给服务的 DoOp() 。
    - 在领导者节点上， rsm 读取协程将 DoOp() 的返回值传递给最初提交请求的 Submit() 协程，随后 Submit() 返回该值。
- 你的服务器不应直接通信；它们应仅通过 Raft 进行交互。
- **注意test中超时时间为1s，设置时设置大一点防止出错**

#### task 4B
##### TestBasic4B
-  使用rsm包来复制一个键/值服务器。每个服务器（“kvservers”）关联一个rsm/Raft对等节点。
    - 客户端向关联的 Raft 是领导者的 kvserver 发送 Put() 和 Get() RPC。
    - kvserver 代码将 Put/Get 操作提交给 rsm
    - rsm 使用 Raft 进行复制，并在每个对等节点上调用你的服务器的 DoOp ,将操作应用到对等节点的键/值数据库中
    - 目的是让服务器维护键/值数据库的相同副本
- Clerk 可能不知道哪个kvserver 是 Raft 领导者，因此会不断发送重试
- 如果键/值服务将操作提交到其 Raft 日志，领导者通过响应其 RPC 向 Clerk 报告结果
    - 如果操作未能提交（例如，如果领导者被替换），服务器会报告错误， Clerk 会使用不同的服务器重试。
- 实现：
    - 实现client.go，添加逻辑觉得rpc请求发送到哪个kvserver 
    - 在server.go中实现Put和Get的RPC处理程序，他们调用rsm.Submit()将请求提交给Raft; 实现DoOp方法供rsm调用
##### unreliable 4B
- 客户端可能需要多次发送 RPC 请求，直到找到能够成功回复的 kvserver。
    - 如果领导者在将条目提交到 Raft 日志后立即失败，客户端可能不会收到回复，因此可能会将请求重新发送给另一个领导者。每次对`Start()`的调用，对于特定的版本号，应仅导致一次执行。
    - **没有实现序列号，但是通过测试了，可能原因在于version机制的存在**

#### task 4C
- 目前没有调用snapshot方法，重启服务器必须重放完整的raft日志才能恢复状态。 因为需要修改kvserver和rsm，使其与raft协作，利用lab 3D中的Raft Snapshot()功能来节省日志空间并减少重启时间。
- 测试程序将 maxraftstate 传递给你的 StartKVServer() ，后者再将其传递给 rsm 。
    - maxraftstate 表示持久化 Raft 状态（包括日志，但不包括快照）允许的最大字节数。
    - 比较 maxraftstate 与 rf.PersistBytes() 。每当你的 rsm 检测到 Raft 状态大小接近此阈值时，应通过调用 Raft 的 Snapshot 来保存快照。
    - rsm可以通过 StateMachine 接口的 Snapshot 方法获取 kvserver 的快照来创建此快照。如果 maxraftstate 为-1，则无需创建快照。
        - maxraftstate 限制适用于 Raft 作为第一个参数传递给 persister.Save() 的 GOB 编码字节。