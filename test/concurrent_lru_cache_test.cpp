#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_lru_cache.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include "../include/rr/concurrent_lru_cache.h"
#include "../include/rr/profiler.h"
using namespace std;

int global_num_thd = 32;
int global_batch = 10000;

void simple_test() {
    rr::LRUCache cache(100, 10);

    for (auto key = 0; key < 13; key++) {
        rr::lru_cache::Page page;
        cache.GetNewPage(page);
        page.key_ = key;
        memset(page.ptr_, '1', 10);
        cache.Write(key, page.ptr_);
    }

    cache.Print();
}
void multi_thd_read_after_write() {
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache  cache(batch * num_thd / 2 * 10, 10);
    // rr::LRUCache  cache(batch * num_thd * 10, 10);

    auto Write = [&cache, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        {
            rr::lru_cache::Page page;
            cache.GetNewPage(page);
            bool is_new = cache.Write(i, page.ptr_);
            // cout << i << endl;
        }
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page page;
            page.key_ = i;
            bool find = cache.Read(page.key_, page.ptr_);
            // cout << i << endl;
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Write, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "ConcurrentLinkedHashMap write time: " << profiler.Micros() << endl;
    // for (auto i = 0; i < map.ThreadNum(); i++) {
    //     map.AsyncQueue()[i]->check();
    // }

    v_thread.clear();
    profiler.Clear();
    profiler.Start();
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Read, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "ConcurrentLinkedHashMap read  time: " << profiler.Micros() << endl;
    // for (auto i = 0; i < map.ThreadNum(); i++) {
    //     map.AsyncQueue()[i]->check();
    // }
    // while(1);
}


int main()
{
    simple_test();
    multi_thd_read_after_write();
    return 0;
}
// g++ concurrent_lru_cache_test.cpp -o concurrent_lru_cache_test.exe -ltbb -lpthread -g