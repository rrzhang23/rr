#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_lru_cache.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <cmath>
#include "../include/rr/concurrent_lru_cache.h"
#include "../include/rr/profiler.h"
#include "../include/rr/random.h"
using namespace std;

int global_num_thd = 28;
size_t global_batch = 10000;


double g_zipf_theta = 0.9;
uint64_t the_n = 0;
double denom = 0;

drand48_data buffer;

double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}

void calculateDenom(size_t max_num) {
    assert(the_n == 0);
    the_n = max_num - 1;
    denom = zeta(the_n, g_zipf_theta);
    cout << "calculateDenom OK!" << endl;
}

double zeta_2_theta = zeta(2, g_zipf_theta);

uint64_t zipf(uint64_t n, double theta) {
    assert(the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
    double u;
    drand48_r(&buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
}














class A {
public:
    size_t* a_{ 0 };
    virtual size_t Ref() = 0;
    virtual size_t RefSize() = 0;
    virtual void Delete() {
        cout << "A::Delete()" << endl;
        delete this;
    }
    ~A() { cout << "~A()" << endl; delete a_; a_ = nullptr; }
};

class B : public A {
public:
    size_t* b_;
    virtual size_t Ref() { (*b_)++; }
    virtual size_t RefSize() { return (*b_); }
    void Delete() {
        cout << "B::Delete()" << endl;
        // delete this;
    }
    ~B() { cout << "~B()" << endl; delete b_; b_ = nullptr; }
};
void test() {
    B* b = new B();
    A* a = b;
    a->Delete();
}


void simple_test() {
    rr::LRUCache* cache = new rr::LRUCache(10 * PAGE_SIZE, PAGE_SIZE);

    for (uint64_t key = 0; key < 13; key++) {
        rr::lru_cache::Page* page = nullptr;
        cache->GetNewPage(page);
        assert(page);
        page->set_key(key); page->set_id(key);
        rr::lru_cache::Page* prior = nullptr;
        bool is_new = cache->Write(page->key(), page, prior);
        if (is_new) { page->Unref(); }
        else { prior->Unref(); }
    }

    cache->Print();
    // cout << __FILE__ << ", " << __LINE__ << endl;
    delete cache;
}

void read_after_write(double den = 1) {
    // cout << "read_after_write     , " << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / den * PAGE_SIZE, PAGE_SIZE);

    auto Write = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            rr::lru_cache::Page* page = nullptr;
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res);
            page->set_key(i); page->set_id(i);
            bool is_new = cache->Write(i, page, prior, thd_id);
            // bool is_new = cache->Write(i, page, prior);
            if (is_new) { page->Unref(); }
            else { prior->Unref(); }
        }
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool find = cache->Read(i, page, thd_id);
            if (find) { page->Unref(); }
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Write, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "read_after_write write time: " << profiler.Micros() << " us" << endl;

    v_thread.clear();
    profiler.Clear();
    profiler.Start();
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Read, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "read_after_write read  time: " << profiler.Micros() << " us" << endl;
    delete cache;
    // cout << "read_after_write done, " << __FILE__ << ", " << __LINE__ << endl;
}

void read_while_write(bool with_delete = true, double den = 1) {
    // cout << "read_while_write, " << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch * num_thd / den * PAGE_SIZE, PAGE_SIZE);

    auto Write = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->GetNewPage(page);
            assert(res);
            page->set_key(i); page->set_id(i);
            bool is_new = cache->Write(i, page, prior, thd_id);
            // bool is_new = cache->Write(i, page, prior);
            if (is_new) { page->Unref(); }
            else { prior->Unref(); }
        }
    };
    auto Read = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* page = nullptr;
            bool find = cache->Read(i, page, thd_id);
            if (!find) {
                rr::lru_cache::Page* page = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(page);
                page->set_key(i); page->set_id(i);
                bool is_new = cache->Write(i, page, prior, thd_id);
                // bool is_new = cache->Write(i, page, prior);
                if (is_new) { page->Unref(); }
                else { prior->Unref(); }
            }
            if (find) { page->Unref(); }
        }
    };
    auto Delete = [&cache, num_thd, batch](int thd_id) -> void
    {
        bool has_res;
        for (int i = 0; i < num_thd * batch; i++)
        {
            rr::lru_cache::Page* prior = nullptr;
            bool res = cache->Delete(i, prior, thd_id);
            if (res) { prior->Unref(); }
        }
    };

    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Write, thd);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_thread.emplace_back(Read, thd);
    }
    if (!with_delete) {
        // cout << cache->cmap_->deleted_size_ << endl;
        // cout << cache->free_list_size_ << endl;
        // cache->cmap_->check();
        // cout << cache->cmap_->deleted_size_ << endl;
        // cout << cache->free_list_size_ << endl;
        for (auto thd = 0; thd < num_thd * 2; thd++) { v_thread[thd].join(); }
    }
    else {
        for (auto thd = 0; thd < num_thd; thd++) { v_thread.emplace_back(Delete, thd); }
        for (auto thd = 0; thd < num_thd * 3; thd++) { v_thread[thd].join(); }
    }
    profiler.End();
    cout << "read_while_write total time: " << profiler.Micros() << " us" << endl;
    delete cache;
    // cout << "read_while_write done, " << __FILE__ << ", " << __LINE__ << endl;
}


enum class TYPE { uniform, ycsb_05, ycsb_06, ycsb_07, ycsb_08, ycsb_09, ycsb_099 };

// 有个 bug, 当调用 Warmup 且 后面不指定线程 thd_id 时, 会卡在 GetNewPage
void lru_test(TYPE type, double den = 1) {
    // cout << "lru_test, " << __FILE__ << ", " << __LINE__ << endl;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::LRUCache* cache = new rr::LRUCache(batch / den * PAGE_SIZE, PAGE_SIZE);

    auto Warmup = [&cache, batch, type]() -> void
    {
        rr::RandNum_generator rng(0, batch - 1);
        bool has_res;
        for (int i = 0; i < batch; i++)
        {
            uint64_t key;
            if (type == TYPE::uniform) { key = rng.nextNum(); }
            else if (type == TYPE::ycsb_05) { key = zipf(batch - 1, 0.5); }
            else if (type == TYPE::ycsb_06) { key = zipf(batch - 1, 0.6); }
            else if (type == TYPE::ycsb_07) { key = zipf(batch - 1, 0.7); }
            else if (type == TYPE::ycsb_08) { key = zipf(batch - 1, 0.8); }
            else if (type == TYPE::ycsb_09) { key = zipf(batch - 1, 0.9); }
            else if (type == TYPE::ycsb_099) { key = zipf(batch - 1, 0.99); }

            rr::lru_cache::Page* p = nullptr;
            bool res = cache->Read(key, p);
            if (!res) {
                rr::lru_cache::Page* page = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(page);
                page->set_key(key); page->set_id(key);
                bool is_new = cache->Write(key, page, prior);
                if (is_new) { page->Unref(); }
                else { prior->Unref(); }
            }
            else {
                p->Unref();
            }
        }
    };

    Warmup();
    Warmup();

    // cout << "Warmup done!" << endl;
    // cout << "cache free size: " << cache->free_list_size_.load() << endl;


    auto Read = [&cache, batch, type](int thd_id) -> void
    {
        bool has_res;
        rr::RandNum_generator rng(0, batch - 1);
        for (int i = 0; i < batch; i++)
        {
            uint64_t key;
            if (type == TYPE::uniform) { key = rng.nextNum(); }
            else if (type == TYPE::ycsb_05) { key = zipf(batch - 1, 0.5); }
            else if (type == TYPE::ycsb_06) { key = zipf(batch - 1, 0.6); }
            else if (type == TYPE::ycsb_07) { key = zipf(batch - 1, 0.7); }
            else if (type == TYPE::ycsb_08) { key = zipf(batch - 1, 0.8); }
            else if (type == TYPE::ycsb_09) { key = zipf(batch - 1, 0.9); }
            else if (type == TYPE::ycsb_099) { key = zipf(batch - 1, 0.99); }

            rr::lru_cache::Page* p = nullptr;
            bool find = cache->Read(key, p, thd_id);
            // bool find = cache->Read(key, p);
            if (!find) {
                rr::lru_cache::Page* page = nullptr;
                rr::lru_cache::Page* prior = nullptr;
                cache->GetNewPage(page);
                page->set_key(key); page->set_id(key);
                bool is_new = cache->Write(key, page, prior, thd_id);
                // bool is_new = cache->Write(key, page, prior);
                if (is_new) { page->Unref(); }
                else { prior->Unref(); }
            }
            else { p->Unref(); }
        }
    };

    // cout << __FILE__ << ", " << __LINE__ << endl;
    rr::Profiler profiler;
    profiler.Start();
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread.emplace_back(Read, thd);
    }
    // cache->PrintRef();
    // cache->cmap_->check();
    // cout << cache->cmap_->deleted_size_ << endl;
    // cout << cache->free_list_size_ << endl;
    for (auto thd = 0; thd < num_thd; thd++)
    {
        v_thread[thd].join();
    }
    profiler.End();
    cout << "lru_test time: " << profiler.Micros() << " us" << endl;

    v_thread.clear();

    delete cache;
    cout << "lru_test done, " << __FILE__ << ", " << __LINE__ << endl << endl;
}


int main()
{
    // simple_test();
    cout << endl << endl << endl;
    // 正确性
    read_after_write(2);
    cout << __FILE__ << ", " << __LINE__ << endl;
    // read_while_write(true, 1.1);      // 有 bug 执行会卡主
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_after_write();
    cout << __FILE__ << ", " << __LINE__ << endl;
    read_while_write();
    cout << __FILE__ << ", " << __LINE__ << endl;
    // read_while_write(false, 1);  // 有 bug，这行执行完，下行执行会卡住,wait_for_asyc_done debug 发现，有一个队列一直有一个值，但是后台线程就是刷不下去，应该是这个原因导致 GetNewPage 卡住
    // cout << __FILE__ << ", " << __LINE__ << endl;
    // read_while_write(false, 2);

    cout << endl << endl << endl;

    calculateDenom(global_batch);
    lru_test(TYPE::uniform, 1);
    lru_test(TYPE::uniform, 5);
    lru_test(TYPE::ycsb_09, 1);
    lru_test(TYPE::ycsb_09, 5);
    return 0;
}
// g++ lru_cache_test3.cpp -o lru_cache_test3.exe -ltbb -lpthread -g