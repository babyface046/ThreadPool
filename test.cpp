#include "FixedThreadPool.hpp"
#include "CachedThreadPool.hpp"
#include "WorkStealingThreadPool.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static std::atomic<int> g_failures{ 0 };

#define CHECK(cond)                                                        \
    do                                                                     \
    {                                                                      \
        if (!(cond))                                                       \
        {                                                                  \
            std::cerr << "FAILED: " #cond " (line " << __LINE__ << ")\n";  \
            g_failures.fetch_add(1, std::memory_order_relaxed);            \
        }                                                                  \
    } while (0)

template <class Pred>
static bool waitFor(Pred pred, int timeoutMs = 5000)
{
    auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeoutMs);
    while (!pred())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

static void testFixedThreadPool()
{
    std::cout << "[FixedThreadPool]\n";
    FixedThreadPool pool(128, 4);

    auto sum = pool.submit([](int a, int b) { return a + b; }, 2, 3);
    CHECK(sum.get() == 5);

    auto text = pool.submit([](const std::string& s) { return s + "!"; },
        std::string("hi"));
    CHECK(text.get() == "hi!");

    auto thrown = pool.submit([]() -> int { throw std::runtime_error("expected"); });
    bool caught = false;
    try
    {
        thrown.get();
    }
    catch (const std::runtime_error&)
    {
        caught = true;
    }
    CHECK(caught);

    std::atomic<int> executed{ 0 };
    std::promise<void> done;
    auto finished = done.get_future();
    pool.excute([&executed, &done]
        {
            ++executed;
            done.set_value();
        });
    CHECK(finished.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    CHECK(executed == 1);

    static constexpr int kTasks = 500;
    std::atomic<int> count{ 0 };
    std::vector<std::future<int>> futures;
    futures.reserve(kTasks);
    for (int i = 0; i < kTasks; ++i)
    {
        futures.push_back(pool.submit([i, &count]
            {
                ++count;
                return i;
            }));
    }

    int mismatched = 0;
    for (int i = 0; i < kTasks; ++i)
    {
        if (futures[i].get() != i)
            ++mismatched;
    }
    CHECK(mismatched == 0);
    CHECK(waitFor([&count] { return count == kTasks; }));
    CHECK(count == kTasks);

    pool.Stop();
    pool.Stop();
    std::cout << "  passed\n";
}

static void testCachedThreadPool()
{
    std::cout << "[CachedThreadPool]\n";
    CachedThreadPool pool(128, 2);

    auto doubled = pool.submit([](int value) { return value * 2; }, 21);
    CHECK(doubled.get() == 42);

    auto text = pool.submit([] { return std::string("cached"); });
    CHECK(text.get() == "cached");

    auto thrown = pool.submit([]() -> int { throw std::logic_error("expected"); });
    bool caught = false;
    try
    {
        thrown.get();
    }
    catch (const std::logic_error&)
    {
        caught = true;
    }
    CHECK(caught);

    std::atomic<int> executed{ 0 };
    std::promise<void> done;
    auto finished = done.get_future();
    pool.excute([&executed, &done]
        {
            ++executed;
            done.set_value();
        });
    CHECK(finished.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
    CHECK(executed == 1);

    static constexpr int kTasks = 500;
    std::atomic<int> count{ 0 };
    std::vector<std::future<int>> futures;
    futures.reserve(kTasks);
    for (int i = 0; i < kTasks; ++i)
    {
        futures.push_back(pool.submit([i, &count]
            {
                ++count;
                return i + 1;
            }));
    }

    int mismatched = 0;
    for (int i = 0; i < kTasks; ++i)
    {
        if (futures[i].get() != i + 1)
            ++mismatched;
    }
    CHECK(mismatched == 0);
    CHECK(waitFor([&count] { return count == kTasks; }));
    CHECK(count == kTasks);

    pool.Stop();
    pool.Stop();
    std::cout << "  passed\n";
}

static void testWorkStealingPool()
{
    std::cout << "[WorkStealingPool]\n";
    WorkStealingPool pool(128, 4);

    static constexpr int kSubmitters = 4;
    static constexpr int kPerSubmitter = 200;
    static constexpr int kTasks = kSubmitters * kPerSubmitter;

    std::atomic<int> count{ 0 };
    std::mutex futuresMutex;
    std::vector<std::future<int>> futures;
    std::vector<std::thread> submitters;

    for (int t = 0; t < kSubmitters; ++t)
    {
        submitters.emplace_back([&pool, &count, &futures, &futuresMutex, t]
            {
                std::vector<std::future<int>> local;
                local.reserve(kPerSubmitter);
                for (int i = 0; i < kPerSubmitter; ++i)
                {
                    int value = t * kPerSubmitter + i;
                    local.push_back(pool.submit([value, &count]
                        {
                            ++count;
                            return value;
                        }));
                }
                std::lock_guard<std::mutex> locker(futuresMutex);
                for (auto& f : local)
                    futures.push_back(std::move(f));
            });
    }

    for (auto& th : submitters)
        th.join();

    CHECK(static_cast<int>(futures.size()) == kTasks);

    long long sum = 0;
    for (auto& f : futures)
        sum += f.get();

    CHECK(sum == static_cast<long long>(kTasks - 1) * kTasks / 2);
    CHECK(waitFor([&count] { return count == kTasks; }));
    CHECK(count == kTasks);

    std::mutex idMutex;
    std::set<std::thread::id> workers;
    std::vector<std::future<void>> probes;
    for (int i = 0; i < 200; ++i)
    {
        probes.push_back(pool.submit([&idMutex, &workers]
            {
                {
                    std::lock_guard<std::mutex> locker(idMutex);
                    workers.insert(std::this_thread::get_id());
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }));
    }
    for (auto& f : probes)
        f.get();

    CHECK(workers.size() >= 2);

    pool.Stop();
    pool.Stop();
    std::cout << "  passed\n";
}

int main()
{
    try
    {
        testFixedThreadPool();
        testCachedThreadPool();
        testWorkStealingPool();
    }
    catch (const std::exception& e)
    {
        std::cerr << "UNEXPECTED EXCEPTION: " << e.what() << "\n";
        return 2;
    }
    catch (...)
    {
        std::cerr << "UNEXPECTED UNKNOWN EXCEPTION\n";
        return 2;
    }

    int failures = g_failures.load();
    if (failures != 0)
    {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }

    std::cout << "ALL TESTS PASSED\n";
    return 0;
}