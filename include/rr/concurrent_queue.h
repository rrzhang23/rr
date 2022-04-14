#ifndef CONCURRENT_QUEUE_H
#define CONCURRENT_QUEUE_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>


namespace rr {
    struct _List_node_base
    {
        _List_node_base* _M_next;

        static void swap(_List_node_base& __x, _List_node_base& __y);
        void _M_transfer(_List_node_base* const __first, _List_node_base* const __last);
        void _M_reverse();
        void _M_hook(_List_node_base* const __position);
        void _M_unhook();
        void init() {
            this->_M_next = nullptr;
        }
        ~_List_node_base() {
            init();
        }
    };

    template <typename _Tp>
    struct list_node : public _List_node_base
    {
        _Tp _M_data;
        _Tp* _M_valptr() { return std::__addressof(_M_data); }
        _Tp const* _M_valptr() const { return std::__addressof(_M_data); }


        list_node(const _Tp& t) : _M_data(t) {
            init();
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        }
        list_node(_Tp&& t) : _M_data(std::move(t)) {
            init();
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        }
        // list_node* operator=(_Tp&& t)
        // {
        //     this->_M_data = std::forward<_Tp>(t); init();
        //     std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        // }

        list_node() { init(); }

        list_node(const list_node& _x) = delete;
        // {
        //     this->_M_data = _x->_M_data;
        //     if (_x->_M_next == _x)
        //         init();
        //     else
        //     {
        //         auto* const __node = this;
        //         __node->_M_next = _x->_M_next;
        //         __node->_M_prev = _x->_M_prev;
        //         __node->_M_next->_M_prev = __node->_M_prev->_M_next = __node;
        //         // _x.init();
        //     }
        // }

        list_node* operator=(const list_node& _x) = delete;

        list_node(list_node&& _x) = delete;
    };

    template <typename _Tp>
    struct _List_iterator
    {
        typedef _List_iterator<_Tp> _Self;
        typedef list_node<_Tp> _Node;

        typedef ptrdiff_t difference_type;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef _Tp value_type;
        typedef _Tp* pointer;
        typedef _Tp& reference;

        _List_iterator() : _M_node() {}

        explicit _List_iterator(_List_node_base* __x) : _M_node(__x) {}

        _Self _M_const_cast() const { return *this; }

        // Must downcast from _List_node_base to _List_node to get to value.
        reference  operator*() const { return *static_cast<_Node*>(_M_node)->_M_valptr(); }

        pointer operator->() const { return static_cast<_Node*>(_M_node)->_M_valptr(); }

        _Self& operator++() {
            _M_node = _M_node->_M_next;
            return *this;
        }

        _Self operator++(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_next;
            return __tmp;
        }

        /**
         * @brief 单向链表，没有 prev
         */
         /*
         _Self& operator--() {
             _M_node = _M_node->_M_prev;
             return *this;
         }

         _Self operator--(int) {
             _Self __tmp = *this;
             _M_node = _M_node->_M_prev;
             return __tmp;
         } */

        bool operator==(const _Self& __x) const { return _M_node == __x._M_node; }

        bool operator!=(const _Self& __x) const { return _M_node != __x._M_node; }

        // The only member points to the %list element.
        _List_node_base* _M_node;
    };

    /**
     *  @brief A list::const_iterator.
     *
     *  All the functions are op overloads.
     */
    template <typename _Tp>
    struct _List_const_iterator
    {
        typedef _List_const_iterator<_Tp> _Self;
        typedef const list_node<_Tp> _Node;
        typedef _List_iterator<_Tp> iterator;

        typedef ptrdiff_t difference_type;
        typedef std::bidirectional_iterator_tag iterator_category;
        typedef _Tp value_type;
        typedef const _Tp* pointer;
        typedef const _Tp& reference;

        _List_const_iterator() _GLIBCXX_NOEXCEPT
            : _M_node() {}

        explicit _List_const_iterator(const _List_node_base* __x) : _M_node(__x) {}

        _List_const_iterator(const iterator& __x) : _M_node(__x._M_node) {}

        iterator _M_const_cast() const {
            return iterator(const_cast<_List_node_base*>(_M_node));
        }

        // Must downcast from List_node_base to _List_node to get to value.
        reference operator*() const {
            return *static_cast<_Node*>(_M_node)->_M_valptr();
        }

        pointer operator->() const {
            return static_cast<_Node*>(_M_node)->_M_valptr();
        }

        _Self& operator++() {
            _M_node = _M_node->_M_next;
            return *this;
        }

        _Self operator++(int) {
            _Self __tmp = *this;
            _M_node = _M_node->_M_next;
            return __tmp;
        }

        /**
         * @brief 单向链表，没有 prev
         */
         /*
         _Self& operator--() {
             _M_node = _M_node->_M_prev;
             return *this;
         }

         _Self operator--(int) {
             _Self __tmp = *this;
             _M_node = _M_node->_M_prev;
             return __tmp;
         } */

        bool operator==(const _Self& __x) const {
            return _M_node == __x._M_node;
        }

        bool operator!=(const _Self& __x) const {
            return _M_node != __x._M_node;
        }

        // The only member points to the %list element.
        const _List_node_base* _M_node;
    };

    template<typename _Tp>
    class ConcurrentQueue {
    public:
        typedef _Tp                          value_type;
        typedef list_node<_Tp>               _node;
        typedef _List_node_base              _node_base;
        typedef _List_iterator<_Tp>          iterator;
        typedef _List_const_iterator<_Tp>    const_iterator;


    private:
        _node_base* head_;    // dummy 节点，不存放 value
        _node_base* tail_;
        std::atomic<size_t> size_;
        // pop 时，size_-- 位置可能有问题，通过 tail_num_ - head_num_ 能看出 size_ 是否正确
        std::atomic<size_t> tail_num_;
        std::atomic<size_t> head_num_;

        // 在初始版本中，pop 后直接删除前一个节点，这会使最后 check() 函数不通过，即从 head_ 到 tail_ 计数不等于 size_
        // 于是便在后台异步删除被 pop 的节点，fake_head_ 到 head_ 之间的节点都是内存存在，但是对外不可见的。
        std::thread async_queue_thread_;
        _node_base* fake_head_;
        bool should_stop_;  // 父线程/进程退出，此时不再需要后台删除
        bool has_shutdown_; // 表示后台线程成功退出，可以正常析构，不然要等后台线程退出才能析构

        // 不用 std::mutex 的原因是，只有内部函数才能上锁，而 thd、pop、push是不能上锁的，这几个函数同时上锁对性能有影响
        // thd、pop、push 只能等待 mutex_ 被释放，因为内部函数要么非做不可，要么时间很短。
        std::atomic<bool> mutex_;
    public:

        void set_stop() { should_stop_ = true; }

        // 表示 fake_head_ 和 head_ 之间有节点需要被删除
        bool can_del() {
            if (fake_head_ == head_ || fake_head_ == tail_) return false;
            return true;
        }
        void thd() {
            while (!should_stop_) {
                while (mutex_.load()) {}    // 可能有 bug
                if(can_del()) {
                    
                    _node_base* p = fake_head_;
                    fake_head_ = fake_head_->_M_next;
                    p->_M_next = nullptr; delete p;
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
            has_shutdown_ = true;
        }
        bool need_thread_safe_;

        ConcurrentQueue() { ConcurrentQueue(true); }

        ConcurrentQueue(bool need_thread_safe) : should_stop_(false), has_shutdown_(false), need_thread_safe_(need_thread_safe) {
            size_.store(0);
            tail_num_.store(0);
            head_num_.store(0);
            head_ = new _node_base();
            head_->init();
            tail_ = head_;
            fake_head_ = head_;

            mutex_.store(false);

            async_queue_thread_ = std::thread(&ConcurrentQueue::thd, this);
            async_queue_thread_.detach();
        }

        ~ConcurrentQueue() {
            should_stop_ = true;
            // 需要等后台线程成功退出
            while (!has_shutdown_);
            should_stop_ = true;
            clear();
            assert(tail_ == head_);
            check();

            delete head_;
            // delete tail_; no need free tail
            head_ = tail_ = nullptr;
            mutex_.store(false);
            // std::cout << "~ConcurrentQueue()" << ", " << __FILE__ << ", " << __LINE__ << std::endl;
        }

        template<typename... _Args>
        void emplace_push(_Args&&... t) {
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
            _node* node = new _node(std::forward<_Tp>(t)...);
            push(node);
        }

        void push(const _Tp& t) {
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
            _node* node = new _node(t);
            push(node);
        }

        void push(_Tp&& t) {
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
            emplace_push(std::move(t));
        }

        void push(_node_base* node) {
            while (mutex_.load()) {}
            // 单线程时，不需要cas
            if (!need_thread_safe_) {
                node->init();
                tail_->_M_next = node;
                tail_ = node;
                size_.fetch_add(1);
                tail_num_.fetch_add(1);
            }
            else {
                node->init();
                _node_base* p = tail_;
                _node_base* oldtail = p;
                do {
                    while (p->_M_next != nullptr) { p = p->_M_next; }
                } while (!__sync_bool_compare_and_swap(&p->_M_next, nullptr, node));
                __sync_bool_compare_and_swap(&tail_, oldtail, node);
                size_.fetch_add(1);
                tail_num_.fetch_add(1);
            }
        }
        bool pop(bool internal_clear = false) {
            value_type value;
            return pop(value, internal_clear);
        }

        bool pop(value_type& value, bool internal_clear = false) {
            if (!internal_clear) {
                while (mutex_.load()) {}
            }
            // 单线程时，不需要
            if (!need_thread_safe_) {
                if (head_ != tail_) {
                    head_ = head_->_M_next;
                    size_.fetch_sub(1);
                    head_num_.fetch_add(1);
                    value = ((_node*)(head_))->_M_data;
                    return true;
                }
                else { return false; }
            }
            else {
                if (size_.load() <= 0) { return false; }
                _node_base* oldhead = head_;
                do {
                    oldhead = head_;
                    if (size_.load() < 0) { assert(false); }
                    if (oldhead == tail_) {
                        return false;
                    }
                } while (!__sync_bool_compare_and_swap(&head_, oldhead, oldhead->_M_next));
                size_.fetch_sub(1);
                value = ((_node*)(oldhead->_M_next))->_M_data;
                head_num_.fetch_add(1);

                return true;
            }
        }

        void clear() {
            mutex_.store(true);

            while (!empty()) { pop(true); }
            assert(head_ == tail_);
            assert(size_.load() == 0);
            assert(size_.load() == tail_num_.load() - head_num_.load());

            while (can_del()) {
                _node_base* p = fake_head_;
                fake_head_ = fake_head_->_M_next;
                p->_M_next = nullptr; delete p;
            }
            assert(fake_head_ == head_);
            mutex_.store(false);
        }

        // 检查 tail_num_-head_num_ 是否等于 size_，
        // 检查 从 head_ 到 tail_, 数量是否等于 size_
        void check() {
            mutex_.store(true);

            assert(tail_->_M_next == nullptr);
            if(size_.load() != tail_num_.load() - head_num_.load()) {
                std::cout << "size: " << size_ << ", tail-num: " << tail_num_.load() - head_num_.load() << std::endl;
                assert(false);
            }

            size_t count = 0;
            for (auto iter = begin(); iter != end(); iter++) { count++; }

            if (count != size_.load()) {
                std::cout << __FILE__ << ": " << __LINE__ << ", count: " << count << ",  size_:" << size_.load() << std::endl;
            }
            assert(count == size_.load());
            mutex_.store(false);
        }

        void print() {
            mutex_.store(true);
            using namespace std;
            cout << "size: " << size_.load() << endl;
            for (auto iter = begin(); iter != end(); iter++) {
                cout << *iter << " " << endl;
            }
            mutex_.store(false);
        }

        iterator begin() { return iterator(head_->_M_next); }
        const_iterator begin() const { return const_iterator(head_->_M_next); }

        iterator end() { return iterator(tail_->_M_next); }
        const_iterator end() const { return const_iterator(tail_->_M_next); }

        bool empty() { return begin() == end(); }

        bool empty_thread_safe() { return size_.load() <= 0; }

        size_t size() {
            return size_.load();
            // return (tail_num_.load() - head_num_.load());
        }
    };
}

#endif // CONCURRENT_QUEUE_H