#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include "../include/rr/concurrent_queue.h"
#include "../include/rr/profiler.h"

using namespace std;


int global_batch = 100000;
int global_num_thd = 32;


void pop_after_push(rr::ConcurrentQueue<int>& queue) {
    assert(queue.empty());

    std::vector<thread> v_threads;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::Profiler profiler;

    if (!queue.need_thread_safe_) {
        batch *= num_thd;
        num_thd = 1;
    }


    atomic<size_t> push_num_;
    push_num_.store(0);
    auto Push = [&queue, batch, &push_num_](int thd_id) -> void {
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            queue.push(i);
            push_num_.fetch_add(1);
            // cout << i << endl;
        }
    };

    atomic<size_t> pop_num_;
    pop_num_.store(0);
    auto Pop = [&queue, batch, &pop_num_](int thd_id) -> void {
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            if (queue.pop()) {
                pop_num_.fetch_add(1);
            }
            // cout << i << endl;
        }
    };

    profiler.Start();
    for (size_t i = 0; i < num_thd; i++) {
        v_threads.emplace_back(Push, i);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_threads[thd].join();
    }
    profiler.End();
    cout << "push time : " << profiler.Micros() << " micros" << endl;
    cout << "push num  : " << push_num_.load() << endl;
    queue.check();
    cout << "push queue.check() done." << endl;


    profiler.Clear();
    v_threads.clear();
    for (size_t i = 0; i < num_thd; i++) {
        v_threads.emplace_back(Pop, i);
    }
    for (auto thd = 0; thd < num_thd; thd++) {
        v_threads[thd].join();
    }
    profiler.End();
    cout << "pop  time : " << profiler.Micros() << " micros" << endl;
    cout << "pop  num  : " << pop_num_.load() << endl;
    queue.check();
    cout << "pop   queue.check() done." << endl << endl;
}

void pop_while_push(rr::ConcurrentQueue<int>& queue) {
    assert(queue.empty());

    std::vector<thread> v_threads;
    int batch = global_batch;
    int num_thd = global_num_thd;
    rr::Profiler profiler;

    if (!queue.need_thread_safe_) {
        batch *= num_thd;
        num_thd = 1;
    }

    atomic<size_t> push_num_;
    push_num_.store(0);
    auto Push = [&queue, batch, &push_num_](int thd_id) -> void {
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            queue.push(i);
            push_num_.fetch_add(1);
            // cout << i << endl;
        }
    };


    atomic<size_t> pop_num_;
    pop_num_.store(0);
    auto Pop = [&queue, batch, &pop_num_](int thd_id) -> void {
        for (int i = thd_id * batch; i < (thd_id + 1) * batch; i++) {
            if (queue.pop()) {
                pop_num_.fetch_add(1);
            }
            // cout << i << endl;
        }
    };

    profiler.Start();
    for (size_t i = 0; i < num_thd; i++)
    {
        v_threads.emplace_back(Push, i);
    }
    for (size_t i = 0; i < num_thd; i++)
    // for (size_t i = 0; i < 1; i++)
    {
        v_threads.emplace_back(Pop, i);
    }
    for (auto thd = 0; thd < num_thd * 2; thd++) {
    // for (auto thd = 0; thd < num_thd + 1; thd++) {
        v_threads[thd].join();
    }
    profiler.End();
    cout << "total time: " << profiler.Micros() << " micros" << endl;
    cout << "push num  : " << push_num_.load() << endl;
    cout << "pop num   : " << pop_num_.load() << endl;
    queue.check();
    cout << "total queue.check() done." << endl << endl;
    profiler.Clear();
}

void simple_test() {
    class A {
    public:
        A() { cout << "A()" << endl; }
        A(const A& a) { cout << "const A&" << endl; }
        A(A&& a) { cout << "A&&" << endl; }
        A* operator=(const A& a) { cout << "const A& operator=" << endl; }
        A* operator=(A&& a) { cout << "A&& operator=" << endl; }
    };
    rr::ConcurrentQueue<A> queue(true);

    A a;
    queue.emplace_push(a);
    std::cout << __FILE__ << ", " << __LINE__ << std::endl << std::endl << std::endl;
    A b;
    queue.push(b);
    std::cout << __FILE__ << ", " << __LINE__ << std::endl << std::endl << std::endl;
    A c;
    queue.push(std::move(c));
}

int main() {
    rr::ConcurrentQueue<int> queue(true);
    assert(false == queue.pop());
    queue.check();

    pop_after_push(queue);

    assert(queue.empty());
    pop_while_push(queue);
    queue.clear();

    simple_test();
    // queue.print();

    return 0;
}
// g++ concurrent_queue_test.cpp -o concurrent_queue_test.exe -lpthread -g