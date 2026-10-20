#ifdef __linux__
#ifndef SCHEDULEDTHREADPOOL_HPP
#define SCHEDULEDTHREADPOOL_HPP

#include "TimerManager.hpp"
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <atomic>
#include <future>
using namespace std;

class ScheduledThreadPool
{
public:
    using Task = std::function<void(void)>;

private:
    TimerManager m_timers;

public:
    ScheduledThreadPool(size_t qusize = 100, int numthreads = 8)
    {
        m_timers.init(-1);
    }
    ~ScheduledThreadPool()
    {
    }

    template <class Func, class... Args>
    void execute(int val, int interval, Func&& func, Args &&...args)
    {
        auto task = std::make_shared<TimerCallback>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
        // Timer te(interval);
        auto te = std::make_shared<Timer>(interval);
        te->init();
        te->set_timer([task]
            { (*task)(); }, val, interval);
        m_timers.add_timer(te);
    }

};

#endif // !SCHEDULEDTHREADPOOL_HPP
#endif