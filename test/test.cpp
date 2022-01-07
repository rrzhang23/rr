#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <thread>
#include <vector>
#include <cassert>
using namespace std;





int main() {


    tbb::concurrent_hash_map<int, int*> map;
    int* a = new int();
    *a = 3;
    map.insert(make_pair(1, a));

    auto fun = [&map]() -> void
    {
        tbb::concurrent_hash_map<int, int*>::accessor acc;
        bool find = map.find(acc, 1);
        if (find) {
            cout << find << endl;
            delete acc->second;
            map.erase(acc);
        }
    };
        auto fun2 = [&map]() -> void
    {
        tbb::concurrent_hash_map<int, int*>::const_accessor acc;
        bool find = map.find(acc, 1);
        if (find) {
            assert(*(acc->second) == 3);
        }
    };
    std::vector<std::thread> v_thread;
    for (auto thd = 0; thd < 32; thd++)
    {
        v_thread.emplace_back(fun);
    }
    for (auto thd = 0; thd < 32; thd++)
    {
        v_thread[thd].join();
    }


    return 0;
} // g++ test.cpp -o test.exe -ltbb -lpthread -g