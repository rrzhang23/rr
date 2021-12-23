#ifndef MYLISH_H
#define MYLISH_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>
#include <cstring>


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


        list_node(const _Tp& t) {
            this->_M_data = t; init();
            // std::cout << __FILE__ << ", " << __LINE__ << std::endl;
        }
        list_node(_Tp&& t) {
            this->_M_data = std::move(t); init();
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
        std::mutex mutex_;
    public:
        bool need_thread_safe_;

        ConcurrentQueue() { ConcurrentQueue(true); };

        ConcurrentQueue(bool need_thread_safe) : need_thread_safe_(need_thread_safe) {
            size_.store(0);
            tail_num_.store(0);
            head_num_.store(0);
            head_ = new _node_base();
            head_->init();
            tail_ = head_;
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
            // 单线程时，不需要cas
            if (!need_thread_safe_) {
                node->init();
                tail_->_M_next = node;
                tail_ = node;
                size_.fetch_add(1);
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
                // tail_num_.fetch_add(1);
            }
        }
        bool pop() {
            value_type value;
            return pop(value);
        }

        bool pop(value_type& value) {
            // 单线程时，不需要
            if (!need_thread_safe_) {
                if (head_ != tail_) {
                    head_ = head_->_M_next;
                    size_.fetch_sub(1);
                    value = ((_node*)(head_))->_M_data;
                    return true;
                }
                else { return false; }
            }
            else {
                if (size_.load() <= 0) { return false; }
                size_.fetch_sub(1);
                _node_base* oldhead;
                do {
                    oldhead = head_;
                    if (size_.load() < 0) { assert(false); }
                    if (oldhead == tail_) {
                        size_.fetch_add(1);
                        return false;
                    }
                } while (!__sync_bool_compare_and_swap(&head_, oldhead, oldhead->_M_next));

                value = ((_node*)(oldhead->_M_next))->_M_data;
                // head_num_.fetch_add(1);

                delete oldhead;
            }
            return true;
        }

        void clear() {
            while (!empty()) { pop(); }
        }

        void check() {
            assert(tail_->_M_next == nullptr);
            // assert(tail_num_.load() - head_num_.load() == size_.load());

            size_t count = 0;
            for (auto iter = begin(); iter != end(); iter++) { count++; }

            std::cout << __FILE__ << ": " << __LINE__ << ", ConcurrentQueue::check(), count: " << count << std::endl;
            if (count != size_.load()) {
                std::cout << __FILE__ << ": " << __LINE__ << ", count: " << count << ",  size_:" << size_.load() << std::endl;
            }
            assert(count == size_.load());
        }

        void print() {
            std::unique_lock<std::mutex> lck(mutex_);
            using namespace std;
            cout << "size: " << size_.load() << endl;
            for (auto iter = begin(); iter != end(); iter++) {
                cout << *iter << " " << endl;
            }
        }

        iterator begin() { return iterator(head_->_M_next); }
        const_iterator begin() const { return const_iterator(head_->_M_next); }

        iterator end() { return iterator(tail_->_M_next); }
        const_iterator end() const { return const_iterator(tail_->_M_next); }

        bool empty() { return begin() == end(); }

        bool empty_thread_safe() { return size_.load() <= 0; }

        size_t size() {
            return size_.load();
        }
    };
}

#endif // MYLISH_H