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

int main()
{
    rr::LRUCache cache(100, 10);

    for (auto key = 0; key < 11; key++) {
        rr::lru_cache::Page page;
        cache.GetNewPage(page);
        page.key_ = key;
        memset(page.ptr_, '1', 10);
        cache.Write(key, page.ptr_);
    }

    cache.Print();
    return 0;
}
// g++ concurrent_lru_cache_test.cpp -o concurrent_lru_cache_test.exe -ltbb -lpthread -g