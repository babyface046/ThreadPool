#pragma once
#include<vector>
#include<list>
#include<deque>
#include<mutex>
#include<condition_variable>
template<class Task>
class FixedThreadPoolSyncQueue
{
private:
	std::list<Task> m_queue;
	mutable std::mutex m_mutex;
	std::condition_variable m_notEmpty;//消费者
	std::condition_variable m_notFull;//生产者
	std::condition_variable m_waitStop;
	size_t m_maxSize;           //任务上限
	size_t m_waitTime = 10;     //10mill
	bool m_Stop;                //是否停止

	bool IsFull()const { return m_queue.size() >= m_maxSize; }
	bool IsEmpty()const { return m_queue.empty(); }
	template<class F>
	int Add(F&& f)
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		while (!m_Stop && IsFull())
		{
			//m_notFull.wait(locker);
			if (std::cv_status::timeout ==
				m_notFull.wait_for(locker, std::chrono::milliseconds(m_waitTime)))
			{
				return -1;
			}
		}
		if (m_Stop)
		{
			return -2;
		}
		m_queue.push_back(std::forward<F>(f));//完美转发
		m_notEmpty.notify_all();
		return 0;
	}

public:
	FixedThreadPoolSyncQueue(size_t maxsize) :m_maxSize(maxsize), m_Stop(false) {};
	~FixedThreadPoolSyncQueue()
	{
		//Stop();
	}
	void Stop()//强制停止
	{
		{
			std::unique_lock<std::mutex>locker(m_mutex);
			m_Stop = true;
		}
		m_notEmpty.notify_all();
		m_notFull.notify_all();
	}
	void WaitStop()
	{
		std::unique_lock<std::mutex> locker(m_mutex);
		while (!IsEmpty())
		{
			//m_waitStop.wait(locker);
			m_waitStop.wait_for(locker, std::chrono::milliseconds(1));
		}
		m_Stop = true;
		m_notEmpty.notify_all();
		m_notFull.notify_all();
	}
	int Put(Task&& task)//生产者
	{
		return Add(std::forward<Task>(task));
	}
	int Put(const Task& task)
	{
		return Add(task);
	}
	void Take(Task& task)//消费者
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		while (!m_Stop && IsEmpty())
		{
			m_notEmpty.wait(locker);
		}
		if (m_Stop)return;
		task = m_queue.front();
		m_queue.pop_front();
		m_notFull.notify_all();
	}
	void Take(std::list<Task>& tasklist)
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		while (!m_Stop && IsEmpty())
		{
			m_notEmpty.wait(locker);
		}
		if (m_Stop)return;
		tasklist = std::move(m_queue);
		m_notFull.notify_all();
	}
	bool Empty()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue.empty();
	}
	bool Full()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue.size() >= m_maxSize;
	}
	size_t Size()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue.size();
	}
	size_t Count()const
	{
		return m_queue.size();
	}
};