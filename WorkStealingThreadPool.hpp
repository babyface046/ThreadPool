#pragma once
#include"WorkStealingThreadPoolSyncQueue.hpp"
#include<functional>
#include<vector>
#include<memory>
#include<thread>
#include<atomic>
#include<iostream>
#include<future>
using namespace std;
class WorkStealingPool
{
public:
	using Task = function<void(void)>;

private:
	WorkStealingThreadPoolSyncQueue<Task> m_taskQueue;
	size_t m_numThreads;
	std::atomic<size_t> m_nextIndex{ 0 };
	std::vector<std::unique_ptr<std::thread>> m_threadgroup;
	std::atomic_bool m_running;
	std::once_flag m_flag;
	int threadIndex()
	{
		return static_cast<int>(
			m_nextIndex.fetch_add(1, std::memory_order_relaxed) % m_numThreads);
	}

	void Start(int numthreads)
	{
		m_running = true;
		for (int i = 0; i < numthreads; ++i)
		{
			m_threadgroup.push_back(std::make_unique<std::thread>(
				&WorkStealingPool::RunInThread, this, i
			));
		}
	}

	void RunInThread(const int  tindex)
	{
		while (m_running)
		{
			std::list<Task> tasklist;
			if (m_taskQueue.Take(tasklist, tindex) == 0)
			{
				for (auto& task : tasklist)
				{
					task();
				}
			}
			else
			{
				int i = threadIndex();
				while (i == tindex)
				{
					i = threadIndex();
				}
				if (m_taskQueue.Take(tasklist, i) == 0)
				{
					clog << " ִ��͵ȡ������ " << endl;
					for (auto& task : tasklist)
					{
						task();
					}
				}
				else
				{
					std::this_thread::yield();
				}
			}
		}
	}

	void StopThreadGroup()
	{
		//m_queue.Stop();
		m_taskQueue.WaitStop();
		m_running = false;
		for (auto& tha : m_threadgroup)
		{
			if (tha && tha->joinable())
			{
				tha->join();
			}
		}
		m_threadgroup.clear();
	}

public:
	WorkStealingPool(int qusize = 100, int numthreads = 8)
		:m_taskQueue(qusize, numthreads),
		m_numThreads(numthreads)
	{
		Start(numthreads);
	}

	~WorkStealingPool()
	{
		Stop();
	}

	void Stop()
	{
		std::call_once(m_flag, [this] {StopThreadGroup(); });
	}

	template<class Func, class...Args>
	auto submit(Func&& func, Args&& ... args)
	{
		using RetType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RetType()>>(
			std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
		);
		std::future<RetType> result = task->get_future();
		if (m_taskQueue.Put([task] {(*task)(); }, threadIndex()) != 0)
		{
			(*task)();
		}

		return result;
	}
	void excute(const Task& task)
	{
		if (m_taskQueue.Put(task, threadIndex()) != 0)
		{
			clog << "not add task queue...." << endl;
			task();
		}

	}
	void excute(Task&& task)
	{
		if (m_taskQueue.Put(std::forward<Task>(task), threadIndex()) != 0)
		{
			clog << "not add task queue...." << endl;
			task();
		}
	}


};