# ThreadPool

C++17 实现的 header-only 线程池集合。

## 功能

| 线程池 | 说明 | 平台 |
| --- | --- | --- |
| `FixedThreadPool` | 固定线程数，有界任务队列 | Windows / Linux |
| `CachedThreadPool` | 按需扩容，空闲线程自动回收 | Windows / Linux |
| `WorkStealingPool` | 每个线程一个任务桶，空闲时窃取任务 | Windows / Linux |
| `ScheduledThreadPool` | `timerfd` + `epoll` + `eventfd` 定时/周期任务 | 仅 Linux |

> 注意：`Timer.hpp`、`TimerManager.hpp`、`ScheduledThreadPool.hpp` 目前为注释状态（待恢复），当前可直接使用的是前三个线程池。

## 文件结构

```
FixedThreadPool.hpp                 固定大小线程池
FixedThreadPoolSyncQueue.hpp        对应的有界阻塞队列
CachedThreadPool.hpp                缓存型线程池
CachedThreadPoolSyncQueue.hpp       对应的有界阻塞队列
WorkStealingThreadPool.hpp          工作窃取线程池
WorkStealingThreadPoolSyncQueue.hpp 多桶任务队列
Timer.hpp                           Linux timerfd 封装
TimerManager.hpp                    Linux epoll 定时器管理
ScheduledThreadPool.hpp             Linux 计划任务线程池
test.cpp                            Windows 三个线程池的测试
ThreadPool.sln / ThreadPool.vcxproj Visual Studio 工程
```

## 环境要求

- C++17
- Windows：MSVC 2022（v143）
- Linux：g++ 9+，链接 `-pthread`

## Windows 构建与测试

在项目根目录执行：

```powershell
cl /std:c++17 /EHsc /I. test.cpp
.\test.exe
```

也可以直接用 Visual Studio 打开 `ThreadPool.sln`，入口为 `test.cpp`。

`test.cpp` 覆盖：

- `submit` 返回值与异常透传
- `excute` 投递
- 大量任务的执行次数与结果校验
- 多线程并发提交（WorkStealing）
- 显式 `Stop()` 与析构

## Linux 构建

前三个线程池可直接使用：

```bash
g++ -std=c++17 -pthread -I. main.cpp -o demo
```

`ScheduledThreadPool` 只支持 Linux，且需要恢复对应源码后才能使用。

## 使用示例

```cpp
#include "FixedThreadPool.hpp"

int main()
{
    FixedThreadPool pool(100, 8);

    auto future = pool.submit([](int a, int b) { return a + b; }, 2, 3);
    int result = future.get();

    pool.excute([] { /* ... */ });

    pool.Stop();
    return 0;
}
```

`submit` 返回 `std::future`；`excute` 用于投递无返回值的任务（方法名保持项目原有拼写）。

## 已知限制

- 队列满时会等待约 10ms，仍失败则在调用线程内联执行任务。
- `CachedThreadPool` 停止时通过 detach 让空闲线程退出，不等待在跑任务结束。
- `WorkStealingPool` 本地桶取不到任务时最多等待 1 秒才尝试窃取。
- `submit` 基于 `std::bind`，不支持 move-only 可调用对象或参数。
- 头文件中使用了 `using namespace std;`。

## 后续计划

- 恢复 Linux 定时器相关实现，并用 `#ifdef __linux__` 做平台隔离
- 增加 CMake 构建
- 补充更多并发与边界测试
