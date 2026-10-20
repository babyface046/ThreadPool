#ifdef __linux__
#ifndef TIMERMAP_HPP
#define TIMERMAP_HPP
#include "Timer.hpp"
#include"CachedThreadPool.hpp"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <vector>
#include <memory>
using namespace std;
class TimerManager
{
private:
    int m_epollfd;
    int m_wakefd;
    std::vector<epoll_event> m_events;
    std::unordered_map<int, std::shared_ptr<Timer>> m_timers;
    mutable std::mutex m_mutex;
    std::atomic_bool m_stop;
    static const int eventsize = 16;
    std::thread m_workThread;
    CachedThreadPool m_mypool;
    void remove_timer(int fd)
    {
        std::lock_guard<std::mutex> locker(m_mutex);
        auto it = m_timers.find(fd);
        if (it == m_timers.end())
            return;
        if (m_epollfd >= 0)
            epoll_ctl(m_epollfd, EPOLL_CTL_DEL, fd, nullptr);
        m_timers.erase(it);
    }

public:
    TimerManager()
        : m_epollfd(-1), m_wakefd(-1), m_stop(false),
        m_mypool(1000, 2)
    {
        m_events.resize(eventsize);
    }
    ~TimerManager()
    {
        set_stop();
    }
    TimerManager(const TimerManager&) = delete;
    TimerManager& operator=(const TimerManager&) = delete;
    bool init(int timerout)
    {
        std::lock_guard<std::mutex> locker(m_mutex);
        if (m_epollfd >= 0)
            return true;
        m_stop = false;
        m_epollfd = epoll_create1(EPOLL_CLOEXEC);
        if (m_epollfd < 0)
        {
            fprintf(stderr, "epoll_create1 failed: %s\n", strerror(errno));
            return false;
        }
        m_wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (m_wakefd < 0)
        {
            fprintf(stderr, "eventfd failed: %s\n", strerror(errno));
            close(m_epollfd);
            m_epollfd = -1;
            return false;
        }
        struct epoll_event ev = { 0 };
        ev.data.fd = m_wakefd;
        ev.events = EPOLLIN | EPOLLET;
        if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, m_wakefd, &ev) < 0)
        {
            fprintf(stderr, "epoll_ctl_add wake fd failed: %s\n", strerror(errno));
            close(m_wakefd);
            m_wakefd = -1;
            close(m_epollfd);
            m_epollfd = -1;
            return false;
        }
        try
        {
            m_workThread = std::thread(&TimerManager::loop, this, timerout);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            close(m_wakefd);
            m_wakefd = -1;
            close(m_epollfd);
            m_epollfd = -1;
            return false;
        }
        return true;
    }
    bool add_timer(const std::shared_ptr<Timer>& tv)
    {
        if (!tv)
            return false;
        std::lock_guard<std::mutex> locker(m_mutex);
        if (m_stop || m_epollfd < 0)
            return false;
        int fd = tv->get_fd();
        if (fd < 0)
            return false;
        struct epoll_event ev = { 0 };
        ev.data.fd = fd;
        ev.events = EPOLLIN | EPOLLET;
        if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, fd, &ev) < 0)
        {
            fprintf(stderr, "epoll_ctl_add failed: %s\n", strerror(errno));
            return false;
        }
        m_timers[fd] = tv;
        return true;
    }
    void loop(int timerout)
    {
        while (!m_stop)
        {
            int n = epoll_wait(m_epollfd, m_events.data(),
                static_cast<int>(m_events.size()), timerout);
            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                fprintf(stderr, "epoll_wait failed: %s\n", strerror(errno));
                break;
            }
            for (int i = 0; i < n; ++i)
            {
                int fd = m_events[i].data.fd;
                if (fd == m_wakefd)
                {
                    std::uint64_t value = 0;
                    while (read(m_wakefd, &value, sizeof(value)) > 0)
                    {
                    }
                    continue;
                }
                std::shared_ptr<Timer> tv;
                {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    auto it = m_timers.find(fd);
                    if (it == m_timers.end())
                        continue;
                    tv = it->second;
                }
                if (!tv)
                    continue;
                m_mypool.excute([tv]
                    { tv->handle_event(); });
                if (tv->IsOnce())
                {
                    remove_timer(fd);
                }
            }
            if (static_cast<size_t>(n) >= m_events.size())
            {
                m_events.resize(m_events.size() * 2);
            }
        }
    }
    void set_stop()
    {
        bool expected = false;
        if (!m_stop.compare_exchange_strong(expected, true))
            return;
        if (m_wakefd >= 0)
        {
            std::uint64_t value = 1;
            if (write(m_wakefd, &value, sizeof(value)) < 0 && errno != EAGAIN)
            {
                fprintf(stderr, "wake eventfd failed: %s\n", strerror(errno));
            }
        }
        if (m_workThread.joinable())
        {
            m_workThread.join();
        }
        std::lock_guard<std::mutex> locker(m_mutex);
        if (m_epollfd >= 0)
        {
            for (const auto& item : m_timers)
            {
                epoll_ctl(m_epollfd, EPOLL_CTL_DEL, item.first, nullptr);
            }
            m_timers.clear();
            close(m_epollfd);
            m_epollfd = -1;
        }
        if (m_wakefd >= 0)
        {
            close(m_wakefd);
            m_wakefd = -1;
        }
    }
};

#endif
#endif