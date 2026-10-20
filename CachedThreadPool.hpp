#pragma once
#include"CachedThreadPoolSyncQueue.hpp"
#include<iostream>
#include<atomic>
#include<functional>
#include<map>
#include<thread>
#include<chrono>
#include<future>
using namespace std;
using namespace std::chrono;
class CachedThreadPool
{
public:
	using Task = function<void(void)>;
private:
	CachedThreadPoolSyncQueue<Task> m_queue;
	std::map<std::thread::id, std::unique_ptr<std::thread>> m_threadgroup;
	std::mutex m_mutex;
	std::condition_variable m_threadExit;
	std::atomic_int m_maxIdleTime{ 5 };    //线程最大空闲时间60s
	std::atomic_int m_maxThreadSize = std::thread::hardware_concurrency() + 1; // 上限
	std::atomic_int m_coreThreadSize{ 2 };// 下限
	std::atomic<int> m_curThreadSize{ 0 }; // 当前线程总数	
	std::atomic<int> m_idleThreadSize{ 0 }; //当前空闲线程数
	std::atomic_bool m_running = false;
	std::once_flag m_flag;

	void Start(int numthreads)
	{
		m_running = true;
		m_curThreadSize = numthreads;
		for (int i = 0; i < numthreads; ++i)
		{
			//auto tha = std::unique_ptr<std::thread>(new std::thread(&CachedThreadPool::RunInThread, this));
			auto tha = std::make_unique<std::thread>(&CachedThreadPool::RunInThread, this);
			std::thread::id tid = tha->get_id();// auto tid = tha->get_id();

			m_threadgroup.emplace(tid, std::move(tha));
			++m_idleThreadSize;

		}
	}
	void RunInThread()
	{
		auto tid = std::this_thread::get_id();
		auto startTime = std::chrono::high_resolution_clock::now();

		while (m_running)
		{
			Task task;
			if (m_queue.Size() == 0)
			{
				auto now = std::chrono::high_resolution_clock::now();
				auto intervalTime = duration_cast<std::chrono::seconds>(now - startTime).count();
				std::unique_lock<std::mutex> locker(m_mutex);
				if (intervalTime >= m_maxIdleTime && m_curThreadSize > m_coreThreadSize)
				{
					m_threadgroup.find(tid)->second->detach();
					m_threadgroup.erase(tid);
					--m_curThreadSize;
					--m_idleThreadSize;
					cout << "idle thread delete " << endl;
					clog << "idle Thread size : " << m_idleThreadSize << endl;
					clog << "cur Thread size : " << m_curThreadSize << endl;
					m_threadExit.notify_one();
					return;
				}
			}

			if (!m_queue.Take(task))
			{
				--m_idleThreadSize;
				task();
				++m_idleThreadSize;
				startTime = std::chrono::high_resolution_clock::now();
			}
		}
	}
	void StopThreadGroup()
	{
		m_queue.WaitStop();
		m_coreThreadSize = 0;
		m_maxIdleTime = 1;
		std::unique_lock<std::mutex> locker(m_mutex);
		while (!m_threadgroup.empty())
		{
			m_threadExit.wait_for(locker, std::chrono::milliseconds(1));
		}
		m_running = false;

	}
	void AddnewThread()
	{
		std::lock_guard<std::mutex> locker(m_mutex);
		if (m_idleThreadSize <= 0 && m_curThreadSize < m_maxThreadSize)
		{
			auto tha = std::make_unique<std::thread>(&CachedThreadPool::RunInThread, this);
			std::thread::id tid = tha->get_id();// auto tid = tha->get_id();

			m_threadgroup.emplace(tid, std::move(tha));
			++m_idleThreadSize;
			++m_curThreadSize;
			cout << "add new thread..." << endl;
			cout << "idle thread size : " << m_idleThreadSize << endl;
			cout << "cur thread size : " << m_curThreadSize << endl;
		}
	}
public:
	CachedThreadPool(int qusize = 200, int numthreads = 2)
		:m_queue(qusize)
	{
		Start(numthreads);
	}
	~CachedThreadPool()
	{
		Stop();
	}
	void Stop()
	{
		std::call_once(m_flag, [&]() {StopThreadGroup(); });

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
			cout << "not add task queue...." << endl;
			(*task)();
		}
		AddnewThread();
		return result;
	}

	void excute(const Task& task)
	{
		if (m_queue.Put(task) != 0)
		{
			cout << "not add task queue...." << endl;
			task();
		}
		AddnewThread();


	}
	void excute(Task&& task)
	{
		if (m_queue.Put(std::forward<Task>(task)) != 0)
		{
			cout << "not add task queue...." << endl;
			task();
		}
		AddnewThread();
	}

};