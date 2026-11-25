// =============================================================================
// server.cpp - Linux Reactor TCP server (epoll + fixed thread pool)
//
// Build & run (Linux, C++17):
//     make
//     ./server 9100 &
//     nc 127.0.0.1 9100        # 多个终端各自输入，均能收到大写回显
//
// 一期：epoll 事件循环 + 线程池 + Reactor 分发（已完成）
// 二期：HTTP 解析与 HTTP/1.1 语义（开发中）
// =============================================================================

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#define EVENTSIZE 1024

// -----------------------------------------------------------------------------
// 工具函数（已写好）
// -----------------------------------------------------------------------------

// 打印错误并退出。perror 会把 errno 对应的原因拼在消息后面。
static void die(const char* msg) {
    perror(msg);
    exit(1);
}

// 把 fd 设成非阻塞。
// 为什么要非阻塞：epoll 通知"可读"之后，read 如果阻塞住，整个线程就卡死了。
static void set_nonblocking(int fd) {
    int old_flag = fcntl(fd, F_GETFL);   // 取出原来的属性
    if (old_flag < 0) die("fcntl F_GETFL");
    int new_flag = old_flag | O_NONBLOCK;  // 加上"非阻塞"这一位
    if (fcntl(fd, F_SETFL, new_flag) < 0) die("fcntl F_SETFL");
}

// 创建监听 socket：socket → bind → listen。
static int create_listener(int port) {
    // 忽略 SIGPIPE：对已关闭的连接写数据会收到这个信号，默认行为是杀掉进程
    signal(SIGPIPE, SIG_IGN);

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) die("socket");

    // SO_REUSEADDR：服务器重启时能立刻复用端口，不用等 TIME_WAIT 结束
    int on = 1;
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) die("setsockopt");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;                 // IPv4
    addr.sin_addr.s_addr = htonl(INADDR_ANY);  // 监听所有网卡
    addr.sin_port = htons(static_cast<uint16_t>(port));  // 主机字节序 → 网络字节序

    if (bind(listenfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) die("bind");
    if (listen(listenfd, 1024) < 0) die("listen");  // 1024 = 未完成连接队列长度

    // ⚠️ listenfd 必须非阻塞：事件循环里的 accept 是"循环收尽直到 EAGAIN"的写法，
    //    阻塞 listenfd 会让 accept 在队列排空时卡死整个事件循环（死锁，不回显）。
    set_nonblocking(listenfd);

    printf("listening on port %d\n", port);
    return listenfd;
}

// =============================================================================
// 线程池
// =============================================================================
//
// 【先说清楚：线程池是干什么的】
//
// 单线程版本：一个主线程干所有事——
//     epoll 收到数据 → 自己 read → 自己处理 → 自己 send 回显
//
// 现在没感觉，因为"转大写"太快了。但 后续"处理"会变慢
// （读磁盘文件、gzip 压缩），一个连接要占主线程几十毫秒，
// 这几毫秒里 epoll 停转 → 其他几百个连接全部卡住。
//
// 线程池的解法：雇 N 个固定工人（worker 线程）在那等着。
//     主线程只干一件事：收。收到数据就把活扔进一个队列，马上回去继续收。
//     worker 们从队列里抢活干，干完回去睡觉。
//
// 为什么叫"池"：工人数量固定（比如 4 个），程序启动时一次建好，反复用。
// 而不是每来一个连接就 pthread_create 一个新线程——创建线程很贵，连接一多就崩。
//
// 所以线程池 = 任务队列 + 锁 + 条件变量：
//     std::queue        → 任务清单（活放这）
//     mutex             → 多个工人同时抢清单会乱，上锁保护
//     condition_variable → 清单空了工人睡觉（不占 CPU），有活 notify 叫醒
//
// =============================================================================
//
// 四个部分：
//
//   1. 成员：
//        std::vector<std::thread>  workers_;   // N 个干活线程
//        std::queue<std::function<void()>> tasks_;  // 任务队列
//        std::mutex                mu_;        // 保护队列
//        std::condition_variable   cv_;        // 没活干时睡在这
//        bool                      stop_ = false;
//
//   2. 构造函数：启动 N 个线程，每个线程跑 worker_loop()
//
//   3. worker_loop()：
//        while (true) {
//            std::unique_lock<std::mutex> lock(mu_);
//            // ⚠️ 必须是 while 不是 if —— 醒了不代表队列里还有货
//            cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
//            if (stop_ && tasks_.empty()) return;   // 退出信号 + 活干完 = 下班
//            auto task = std::move(tasks_.front());
//            tasks_.pop();
//            lock.unlock();          // 提前解锁，别拿着锁干活
//            task();                 // 执行（外面包 try/catch，防止异常把线程搞死）
//        }
//
//   4. submit(task)：加锁 → push → unlock → cv_.notify_one()
//
//   5. 析构函数：stop_ = true; cv_.notify_all(); 然后 join 所有线程
//
// 它就是线程池的原型：任务队列 = 产品队列，worker 线程 = 消费者。
//
// 验收：100 个任务交给 4 个线程跑完，程序能正常退出不卡住。
// =============================================================================

class ThreadPool {
public:
    explicit ThreadPool(int n) {
        workers_.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i)
            workers_.emplace_back([this] { worker_loop(); });
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mu_);   // 先置标志再解锁
            stop_ = true;
        }
        cv_.notify_all();                            // 解锁后再叫醒，避免"醒来抢不到锁又睡回去"

        for (auto& t : workers_)
            if (t.joinable()) t.join();
    }

    // 禁止拷贝：线程和 mutex 都不该被复制
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mu_);
                // 谓词用 stop_ || !tasks_.empty()：
                // 被唤醒后条件可能已被别的 worker 抢走，必须重新检查
                cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

                // 两个条件都满足才下班：只写 stop_ 会把队列里剩下的活丢掉
                if (stop_ && tasks_.empty()) return;

                task = std::move(tasks_.front());
                tasks_.pop();
            }   // ← 这里解锁，task() 在锁外执行，N 个 worker 才真并行

            try {
                task();
            } catch (...) {
                // 单个任务抛异常不能带走 worker 线程，否则线程池会越跑越少
            }
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;
};

// =============================================================================
// epoll 事件循环
// =============================================================================
//
// 实现要点：
//
//   1. int epfd = epoll_create(1);
//        → 创建一本"花名册"，返回这本册子的编号
//
//   2. 把 listenfd 登记进册子：
//        epoll_event ev{};
//        ev.events = EPOLLIN;        // 关心"可读"事件
//        ev.data.fd = listenfd;      // 记下这个 fd 是谁
//        epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);
//
//   3. while (true) 循环：
//        epoll_event events[1024];
//        int n = epoll_wait(epfd, events, 1024, -1);   // -1 = 一直等到有事件
//        // n = 这次有几个 fd 就绪（只有就绪的才会进 events 数组，这就是 epoll 快的本质）
//
//        for (int i = 0; i < n; ++i) {
//            int fd = events[i].data.fd;
//
//            if (fd == listenfd) {
//                // 【新连接】accept，拿到 connfd 后必须：
//                //   a) set_nonblocking(connfd)
//                //   b) epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &ev)  ← 漏了这步，
//                //      这个连接之后发的数据永远收不到
//            } else {
//                // 【已连接客户端发来数据】
//                char buf[1024];
//                ssize_t len = recv(fd, buf, sizeof(buf), 0);
//                if (len > 0)      send(fd, buf, len, 0);   // 回显
//                else if (len == 0) { /* 对端关闭 → close(fd) */ }
//                else              { /* -1：出错或 EAGAIN，处理一下 */ }
//            }
//        }
//
// 三个必须记住的点：
//   · epoll_wait 返回的 n 是"就绪的 fd 个数"，不是"总 fd 数"
//   · recv 返回 0 不是错误，是客户端正常断开了
//   · 新 connfd 一定要再 epoll_ctl(ADD) 一次，这是新手最常漏的一步
//
// 验收：开两个终端 nc 127.0.0.1 9100，各自输入文字都要能回显。
// =============================================================================

// 按 fd 取模分片的写锁：保证同一连接上的 send 串行，报文不会交错。
// （fd 复用竞争：后续用 shared_ptr<Connection> 解决）
static constexpr int kWriteLockStripes = 64;
static std::mutex g_write_locks[kWriteLockStripes];

static std::mutex& write_lock_of(int fd) {
    return g_write_locks[static_cast<size_t>(fd) % kWriteLockStripes];
}

static void run_event_loop(int listenfd) {
    ThreadPool pool(4);   // 4 个 worker：主线程只负责收事件

    int epfd = epoll_create1(0);
    if (epfd < 0) die("epoll_create1");

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listenfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev) < 0) die("epoll_ctl listenfd");

    while (true) {
        epoll_event events[EVENTSIZE];
        int n = epoll_wait(epfd, events, EVENTSIZE, -1);
        if (n < 0) {
            if (errno == EINTR) continue;   // 被信号打断，重新等
            die("epoll_wait");
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            if (fd == listenfd) {
                // 【新连接】while 循环把这一轮到达的连接一次收尽，减少 epoll 唤醒次数
                while (true) {
                    sockaddr_in clnt_addr{};
                    socklen_t len = sizeof(clnt_addr);
                    int connfd = accept(listenfd, reinterpret_cast<sockaddr*>(&clnt_addr), &len);
                    if (connfd < 0) {
                        // ⚠️ 单个连接 accept 失败绝不能让整个服务器退出
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 收尽了
                        if (errno == EINTR) continue;                        // 被信号打断，重试
                        perror("accept");                                    // 只打印，继续跑
                        break;
                    }

                    set_nonblocking(connfd);   // 必须非阻塞：交给线程池后，阻塞读会卡死整个 worker

                    epoll_event cev{};
                    cev.events = EPOLLIN;
                    cev.data.fd = connfd;
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &cev) < 0) {
                        perror("epoll_ctl ADD");
                        close(connfd);
                        continue;
                    }

                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &clnt_addr.sin_addr, ip, sizeof(ip));
                    printf("connect %s:%u (fd=%d)\n", ip, ntohs(clnt_addr.sin_port), connfd);
                }
                continue;
            }

            // 对端异常断开 / 出错
            if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                char buf[BUFSIZ];   // 每次循环独立：引入线程池后，共享 buf 会数据竞争
                ssize_t ret = recv(fd, buf, sizeof(buf), 0);

                if (ret < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
                    perror("recv");
                    // ⚠️ 顺序：先从 epoll 摘掉，再 close。反过来会误删复用同一 fd 号的新连接
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    continue;
                }
                if (ret == 0) {     // 0 不是错误，是客户端正常断开
                    printf("client fd=%d closed\n", fd);
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    continue;
                }

                write(STDOUT_FILENO, buf, ret);

                // 【核心】把"处理 + 写回"整体丢给线程池，主线程马上回去 epoll_wait。
                // data 必须按值捕获：buf 是栈变量，submit 一返回就失效了。
                std::string data(buf, static_cast<size_t>(ret));
                pool.submit([fd, data = std::move(data)]() mutable {
                    for (char& c : data)
                        c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

                    std::lock_guard<std::mutex> lk(write_lock_of(fd));
                    ssize_t sent = send(fd, data.data(), data.size(), 0);
                    if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                        perror("send");
                });
            }
        }
    }
}

// -----------------------------------------------------------------------------
// main（已写好）
// -----------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    int port = (argc > 1) ? atoi(argv[1]) : 9100;
    int listenfd = create_listener(port);
    run_event_loop(listenfd);
    close(listenfd);
    hello
    return 0;

}
