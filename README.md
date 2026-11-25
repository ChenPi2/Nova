# Nova · Linux 网络并发框架（一期：Reactor 核心）

基于 epoll 与线程池实现的 Reactor 模型 TCP 网络层。主线程负责事件监听与读取，业务处理封装为任务投递线程池，实现 I/O 与计算解耦。

**当前状态**：一期并发核心已完成；HTTP 层开发中。

## 已实现

- 非阻塞监听 + `accept` 收尽循环，处理 `EAGAIN` / `EWOULDBLOCK` / `EINTR` / `SIGPIPE`，配置 `SO_REUSEADDR`
- epoll 事件循环（LT）：主线程只 `recv`，任务以 `std::function<void()>` 投递线程池
- 固定大小线程池：`std::queue` + `std::mutex` + `std::condition_variable`（`while` 谓词防虚假唤醒）、任务异常兜底、花括号缩小临界区
- 按 fd 取模的 64 路写锁分片，避免同一连接被多个 worker 并发 `send` 导致数据交错

## 构建与运行

需要 Linux（依赖 `epoll`）、g++（C++17）、`nc` 用于验证。

```bash
make
./server 9100 &
```

单客户端验证：

```bash
nc 127.0.0.1 9100
# 输入任意文字，收到转大写后的回显
```

并发验证：

```bash
for i in $(seq 1 10); do (echo "hello$i" | nc 127.0.0.1 9100) & done; wait
```

10 个客户端各自收到完整的大写回显、无数据交错即通过。

## 设计取舍

- **固定大小线程池**：服务器线程上限是 CPU 核数量级，空闲线程阻塞在条件变量上不消耗 CPU；动态缩容需要维护 dying 额度与唤醒匹配，收益小于引入并发 bug 的风险。
- **LT + 读写分离**：主线程只 `recv`、worker 只 `send`，同一 fd 不会被两个线程同时读，现阶段不需要 `EPOLLONESHOT`；后续引入连接状态对象时再补。
- **单文件编译**：一期保持单文件、零外部依赖（仅标准库 + pthread），便于逐层演进；二期拆分为 net / http / web 三层并引入 CMake 与单元测试。

## 已知限制

- 非阻塞 `send` 的部分写尚未做输出缓冲区
- 裸 fd 在 `close` 后被复用时存在事件误删竞争，二期用 `shared_ptr<Connection>` 解决
- 暂无连接超时机制，需与 keep-alive 一并补齐

## 后续计划

HTTP 解析 → HTTP/1.1 语义（keep-alive / Range 206 / ETag 304）→ 路由与中间件 → 工程化与压测
