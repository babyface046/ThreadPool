#pragma once
#include"FixedThreadPoolSyncQueue.hpp"
#include<functional>
#include<iostream>
#include<memory>
#include<thread>
#include<atomic>
#include<future>
using namespace std;

class FixedThreadPool
{
public:
	using Task = std::function<void(void)>;
private:
	FixedThreadPoolSyncQueue<Task>m_queue;
	std::list<std::unique_ptr<std::thread>>m_threadgroup;
	std::atomic_bool m_running; //当前线程池是否处于运行状态
	std::once_flag m_flag;
	void Start(int numthreads)
	{
		m_running = true;
		for (int i = 0; i < numthreads; i++)
		{
			m_threadgroup.push_back(std::make_unique<std::thread>(
				&FixedThreadPool::RunInThread, this));
		}
	}

	void RunInThread()
	{
		while (m_running)
		{
			Task task;
			m_queue.Take(task);
			if (task && m_running)
			{
				task();
			}
		}

	}

	void StopThreadGroup()
	{
		//m_queue.Stop();
		m_queue.WaitStop();
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
	FixedThreadPool(size_t qusize = 100, int numthreads = 8)
		:m_queue(qusize), m_running(false)
	{
		Start(numthreads);
	}
	~FixedThreadPool()
	{
		Stop();
	}
	void Stop()
	{
		std::call_once(m_flag, [this]()->void {StopThreadGroup(); });
	}

	template<class Func, class...Args>
	auto submit(Func&& func, Args&& ... args)
	{
		using RetType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RetType()>>(
			std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
		);
		std::future<RetType> result = task->get_future();
		if (m_queue.Put([task] {(*task)(); }) != 0)
		{
			(*task)();
		}
		/*std::packaged_task<RetType()>
			task(std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
		std::future<RetType> result = task.get_future();
		if (m_queue.Put(task) != 0)
		{
			task();
		}*/
		return result;
	}





	void excute(const Task& task)
	{
		if (m_queue.Put(task) != 0)
		{
			cout << "not add task queue...." << endl;
			task();
		}

	}
	void excute(Task&& task)
	{
		if (m_queue.Put(std::forward<Task>(task)) != 0)
		{
			cout << "not add task queue...." << endl;

			task();
		}
	}


};

