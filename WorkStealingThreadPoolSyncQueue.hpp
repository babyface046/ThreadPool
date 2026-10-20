#pragma once
#include<vector>
#include<list>
#include<deque>
#include<map>
#include<unordered_map>
#include<mutex>
#include<condition_variable>
#include<memory>
#include<atomic>
template<class Task>
class WorkStealingThreadPoolSyncQueue
{
private:
	//std::list<Task> m_queue;
	std::vector<std::list<Task>> m_queue;  //每个桶单独上锁
	size_t m_bucketSize;         //桶的数量
	size_t m_maxSize;             //每个桶的任务上限
	//mutable std::mutex m_mutex;
	std::unique_ptr<std::mutex[]> m_mutexs;
	std::condition_variable m_notEmpty;//消费者
	std::condition_variable m_notFull;//生产者
	std::condition_variable m_waitStop;
	size_t m_waitTime = 10;     //10mill
	std::atomic_bool m_Stop;                //是否停止

	size_t TotalTaskCount()const
	{
		size_t num = 0;
		for (auto& tlist : m_queue)
		{
			num += tlist.size();
		}
		return num;
	}

	bool IsFull(int index)const { return m_queue[index].size() >= m_maxSize; }
	bool IsEmpty(int index)const { return m_queue[index].empty(); }
	template<class F>
	int Add(F&& f, int index)
	{
		std::unique_lock<std::mutex>locker(m_mutexs[index]);
		while (!m_Stop && IsFull(index))
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
		m_queue[index].push_back(std::forward<F>(f));//完美转发
		m_notEmpty.notify_all();
		return 0;
	}

public:
	WorkStealingThreadPoolSyncQueue(size_t maxsize, size_t bucketsize)
		:m_maxSize(maxsize), m_Stop(false), m_bucketSize(bucketsize)
	{
		m_queue.resize(bucketsize);
		m_mutexs = std::make_unique<std::mutex[]>(m_bucketSize);
	}
	~WorkStealingThreadPoolSyncQueue()
	{
		//Stop();
	}

	void Stop()//强制停止
	{
		{
			//std::unique_lock<std::mutex>locker(m_mutex);
			m_Stop = true;
		}
		m_notEmpty.notify_all();
		m_notFull.notify_all();
	}


	void WaitStop()
	{
		for (int i = 0; i < m_bucketSize; ++i)
		{
			std::unique_lock<std::mutex> locker(m_mutexs[i]);
			while (!IsEmpty(i))
			{
				//m_waitStop.wait(locker);
				m_waitStop.wait_for(locker, std::chrono::milliseconds(1));
			}

		}
		m_Stop = true;
		m_notEmpty.notify_all();
		m_notFull.notify_all();
	}

	int Put(Task&& task, int index)//生产者
	{
		return Add(std::forward<Task>(task), index);
	}

	int Put(const Task& task, int index)
	{
		return Add(task, index);
	}

	int Take(Task& task, int index)//消费者
	{
		std::unique_lock<std::mutex>locker(m_mutexs[index]);
		while (!m_Stop && IsEmpty(index))
		{
			//m_notEmpty.wait(locker);
			if (std::cv_status::timeout == m_notEmpty.wait_for(locker, std::chrono::seconds(1)))
			{
				return -1;
			}

		}
		if (m_Stop)return -2;
		task = m_queue[index].front();
		m_queue[index].pop_front();
		m_notFull.notify_all();
		return 0;
	}

	int Take(std::list<Task>& tasklist, int index)
	{
		std::unique_lock<std::mutex>locker(m_mutexs[index]);
		while (!m_Stop && IsEmpty(index))
		{
			//m_notEmpty.wait(locker);
			if (std::cv_status::timeout == m_notEmpty.wait_for(locker, std::chrono::seconds(1)))
			{
				return -1;
			}
		}
		if (m_Stop)return -2;
		tasklist = std::move(m_queue[index]);
		m_notFull.notify_all();
		return 0;
	}
	/*
	bool Empty()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return TotalTaskCount() == 0;
	}

	bool Full()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return TotalTaskCount() >= m_maxSize * m_bucketSize;
	}

	bool BucketEmpty(int index)const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue[index].empty();
	}

	bool BucketFull(int index)const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue[index].size() >= m_maxSize;
	}

	size_t BucketSize(int index)const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return m_queue[index].size();
	}

	size_t Size()const
	{
		std::unique_lock<std::mutex>locker(m_mutex);
		return TotalTaskCount();
	}
	*/

	size_t Count()const
	{
		return TotalTaskCount();
	}
};
