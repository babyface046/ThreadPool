#ifdef __linux__
#ifndef TIMER_HPP
#define TIMER_HPP
#include <sys/timerfd.h>
#include <unistd.h>
#include <string.h>
#include <functional>
#include <iostream>
#include <cstdint>
using namespace std;
using TimerCallback = std::function<void(void)>;
class Timer
{
public:
private:
    int m_fd;
    TimerCallback m_callback;
    bool m_tag; // 0 once   1 interval
    bool settimer(const size_t val, const size_t interval)
    {
        bool ret = true;
        struct itimerspec new_value = { 0 };
        new_value.it_value.tv_sec = val;
        new_value.it_value.tv_nsec = 1;
        new_value.it_interval.tv_sec = interval;
        new_value.it_interval.tv_nsec = 0;
        if (timerfd_settime(m_fd, 0, &new_value, nullptr) < 0)
        {
            ret = false;
            fprintf(stderr, "settimer failed: %s\n", strerror(errno));
        }
        return ret;
    }

public:
    Timer(int tag = 1) : m_tag(tag), m_fd(-1), m_callback(nullptr) {}
    ~Timer() { delete_timer(); }
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;
    Timer(Timer&& other) : m_tag(other.m_tag), m_fd(other.m_fd), m_callback(other.m_callback)
    {

        other.m_fd = -1;
        other.m_callback = nullptr;
    }
    Timer& operator=(Timer&& other)
    {
        if (this == &other)
            return *this;
        delete_timer();
        m_tag = other.m_tag;
        m_fd = other.m_fd;
        m_callback = other.m_callback;
        other.m_fd = -1;
        other.m_callback = nullptr;
        return *this;
    }
    bool init()
    {
        bool ret = true;
        if (m_fd > 0)
        {
            return ret;
        }
        m_fd = timerfd_create(CLOCK_MONOTONIC, 0);
        if (m_fd < 0)
        {
            ret = false;
        }
        return ret;
    }
    bool set_timer(const TimerCallback& cb, const size_t val, const size_t interval)
    {
        bool ret = false;
        if (m_fd > 0 && settimer(val, interval))
        {
            m_callback = cb;
            ret = true;
        }
        return ret;
    }
    bool reset_timer(const size_t val, const size_t interval)
    {
        bool ret = false;
        if (m_fd > 0 && m_callback != nullptr && settimer(val, interval))
        {
            ret = true;
        }
        return ret;
    }
    bool delete_timer()
    {
        bool ret = false;
        if (m_fd > 0)
        {
            close(m_fd);
            m_fd = -1;
            m_callback = nullptr;
            ret = true;
        }
        return ret;
    }
    void handle_event()
    {
        std::uint64_t expire_cnt = 0;
        if (read(m_fd, &expire_cnt, sizeof(expire_cnt)) != sizeof(expire_cnt))
        {
            return;
        }
        if (m_callback != nullptr)
        {
            m_callback();
        }
    }
    int get_fd() const
    {
        return m_fd;
    }
    bool IsOnce() const { return !m_tag; }
};

#endif
#endif