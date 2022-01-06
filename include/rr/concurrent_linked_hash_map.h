#ifndef CONCURRENT_LINKED_HASH_MAP
#define CONCURRENT_LINKED_HASH_MAP

#include <tbb/concurrent_hash_map.h>
#include <iostream>
#include <cassert>
#include <mutex>
#include <forward_list>
#include <atomic>
#include <thread>
#include <map>
#include <queue>
#include "list.h"
#include "concurrent_queue.h"
using namespace std;

#define DEFAULT_MAX_SIZE SIZE_MAX

namespace rr {
	class Hint {
	public:
		virtual void Delete(void* arg1) = 0;
		virtual void New(void* arg1) = 0;
	};

	template <typename KEY, typename VALUE, class Manager = Hint, typename Allocator = std::allocator<std::pair<KEY, VALUE>>>
	class ConcurrentLinkedHashMap;


	namespace con_map {
		template<typename KEY, typename VALUE, class Manager>
		class HandleBase {
		public:
			template<typename K, typename V, class M, typename A> friend class ConcurrentLinkedHashMap;
			/* or: */
			// friend class ConcurrentLinkedHashMap<KEY, VALUE, Manager>;
			typedef ConcurrentLinkedHashMap<KEY, VALUE, Manager> Map;

			virtual ~HandleBase() {};

			HandleBase(Map* map, const KEY& k, const VALUE& v) : mapParent_(map), key_(k), value_(v), weight_(1) {}
			HandleBase(Map* map, const KEY& k, VALUE&& v) : mapParent_(map), key_(k), value_(std::move(v)), weight_(1) {}

			const HandleBase& operator=(const HandleBase& handle) = delete;
			const HandleBase& operator=(HandleBase&& handle) = delete;
			HandleBase(const HandleBase& handle) = delete;
			HandleBase(HandleBase&& handle) = delete;
			HandleBase() = delete;

		protected:
			Map* mapParent_;
			KEY key_;
			VALUE value_;
			int weight_;
		public:
			VALUE& value() { return value_; }
			KEY& key() { return key_; }
		};

		template<typename KEY, typename VALUE, class Manager>
		class Handle : public HandleBase<KEY, VALUE, Manager> {
		public:
			template<typename K, typename V, typename M, typename A> friend class ConcurrentLinkedHashMap;
			typedef ConcurrentLinkedHashMap<KEY, VALUE, Manager> Map;
			typedef typename ConcurrentLinkedHashMap<KEY, VALUE, Manager>::QueueNode QueueNode;
			using handle_base_type = HandleBase<KEY, VALUE, Manager>;

		private:
			::list_node node_;
			QueueNode* queueNode_;
		protected:

		public:
			void copy(::list_node* node) {
				if (node->next == node) {
					assert(node->prev == node);
					INIT_LIST_HEAD(node);
				}
				else {
					::list_node* prev = node->prev;
					::list_node* next = node->next;
					this->node.prev = prev;
					prev->next = this->node;
					this->node.next = next;
					next->prev = this->node;
				}
			}

			virtual ~Handle() {
				node_.prev = nullptr;
				node_.next = nullptr;
				// no need free mapParent_ and queueNode_
			}

			Handle(Map* map, const KEY& k, const VALUE& v) : handle_base_type::HandleBase(map, k, v) {
				INIT_LIST_HEAD(&node_);
			}


			Handle(Map* map, const KEY& k, VALUE&& v) : handle_base_type::HandleBase(map, k, std::move(v)) {
				INIT_LIST_HEAD(&node_);
			}

			const Handle& operator=(const Handle& handle) = delete;
			// {
			// 	INIT_LIST_HEAD(&node);
			// 	this->mapParent_ = handle.mapParent_;
			// 	this->key = handle.key;
			// 	copy(&handle.node);
			// 	return *this;
			// }
			const Handle& operator=(Handle&& handle) = delete;
			Handle(const Handle& handle) = delete;
			// {
			// 	INIT_LIST_HEAD(&node);
			// 	this->mapParent_ = handle.mapParent_;
			// 	this->key = handle.key;
			// 	copy(&handle.node);
			// }
			Handle() = delete;
		};
	}

	template <typename KEY, typename VALUE, class Manager, typename Allocator>
	class ConcurrentLinkedHashMap {
	public:
		template<typename K, typename V, typename M> friend class HandleBase;

		typedef KEY key_type;
		typedef VALUE value_type;

		/* or: */
		// typedef Handle<KEY, VALUE> Handle_type;
		// typedef tbb::concurrent_hash_map<KEY, Handle*> ConcurrentMap;
		// typedef typename ConcurrentMap::accessor accessor;
		// typedef typename ConcurrentMap::const_accessor const_accessor;
		using handle_base_type = con_map::HandleBase<KEY, VALUE, Manager>;
		using handle_type = con_map::Handle<KEY, VALUE, Manager>;
		using ConcurrentMap = tbb::concurrent_hash_map<KEY, handle_base_type*>;
		using accessor = typename ConcurrentMap::accessor;
		using const_accessor = typename ConcurrentMap::const_accessor;


	public:
		struct QueueNode;

	private:
		Manager* manager_;
		bool should_stop_;

		// 上层接口同步修改 map_，被删除的节点会放到异步驱逐队列 deleted_queue_, Handle* 被后台线程 delete；put 时 Handle* 被 new
		tbb::concurrent_hash_map<KEY, handle_base_type*> map_;
		size_t max_size_{ DEFAULT_MAX_SIZE };

		/**
		 * @brief 修改完 map_ 后，修改链表 in_use_list_ 异步操作，先把操作记录进 async_queue_，在慢慢合并至 in_use_list_。
		 * async_queue_ 是个数组 async_queue_[CPU_num]，避免多线程访问同一个 ConcurrentQueue 竞争
		 * async_queue_mutex_ 很少用
		 */
		enum class OP { PUT, GET, UPDATE, DEL };
		rr::ConcurrentQueue<QueueNode>** async_queue_;
		size_t thread_num_;
		std::mutex async_queue_mutex_;
		thread async_queue_thread_;

		/**
		 * @brief 先进先出列表，read 操作会把已经在里面的取到最后面，write 会直接追加至后面，从前面 pop 删除
		 * in_use_list_mutex_ 用于保护这几个成员
		 */
		::list_node in_use_list_;
		std::atomic<size_t> in_use_list_size_;
		std::mutex in_use_list_mutex_;

		rr::ConcurrentQueue<handle_base_type*> deleted_queue_;
		std::atomic<size_t> deleted_size_;
		thread deleted_queue_thread_;


		void AsyncRWListThread() {
			int count = 0;

			while (!should_stop_) {
				for (auto i = 0; i < thread_num_; i++) {
					// if (!async_queue_[i]->empty()) {
					QueueNode front;
					bool res = async_queue_[i]->pop(front);
					if (res) {
						std::unique_lock<std::mutex> lck(in_use_list_mutex_);
						use(front);
					}
					// }
					else {
						// sleep(1);
					}
				}
				count++;
			}
			return;
		}

		// void AsyncDeleteDQueueThread() {
		// 	while (process_state) {
		// 		LRUHandle* tmp = deleted_queue_.pop();
		// 		if (tmp != nullptr) {
		// 			assert(!tmp->weightedValue_->isAlive());
		// 			delete tmp;
		// 			tmp = nullptr;
		// 		}
		// 	}
		// 	return;
		// }

	public:

	static char OPToChar(OP op){
		if(op == OP::PUT) { return 'P'; }
		if(op == OP::GET) { return 'G'; }
		if(op == OP::DEL) { return 'D'; }
		if(op == OP::UPDATE) { return'U'; }
		return 'N';
	}

		ConcurrentLinkedHashMap() { ConcurrentLinkedHashMap(SIZE_MAX); };
		ConcurrentLinkedHashMap(size_t max_size) : in_use_list_size_(0), thread_num_(std::thread::hardware_concurrency())
			, deleted_queue_(true), should_stop_(false) {
			INIT_LIST_HEAD(&in_use_list_);
			assert(in_use_list_size_ <= max_size_);
			max_size_ = max_size;

			async_queue_ = new rr::ConcurrentQueue<QueueNode>*[thread_num_];
			for (auto i = 0; i < thread_num_; i++) {
				async_queue_[i] = new rr::ConcurrentQueue<QueueNode>(true);
				assert(async_queue_[i] != nullptr);
			}
			// std::cout << thread_num_ << " cores" << std::endl;

			async_queue_thread_ = thread(&ConcurrentLinkedHashMap::AsyncRWListThread, this);
			async_queue_thread_.detach();
		}
		~ConcurrentLinkedHashMap() {
			if (!should_stop_) { should_stop_ = true; }
			deleted_queue_.clear();
			for (auto i = 0; i < thread_num_; i++) { delete async_queue_[i]; }
		}

		bool Put(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return         put(key, value, false, prior, thread_id); }
		bool Put(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return              put(key, std::move(value), false, prior, thread_id); }

		bool PutIfAbsent(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return put(key, value, true, prior, thread_id); }
		bool PutIfAbsent(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return      put(key, std::move(value), true, prior, thread_id); }

		/**
		 * @brief
		 *
		 * @param key
		 * @param prior
		 * @param thread_id
		 * @return true, has old value and delete successfully
		 * @return false
		 */
		bool Remove(const KEY& key, VALUE& prior, int thread_id = 0) {
			accessor acc;
			bool find = map_.find(acc, key);
			handle_base_type* handle = nullptr;
			if (find) {
				handle = acc->second;
				bool deleted = map_.erase(acc);

				prior = handle->value_;
				handle->weight_ = 0;
				afterTask(handle, OP::DEL, thread_id);
			}

			return find;
		}

		bool Get(const KEY& key, VALUE& result, int thread_id = 0) {
			const_accessor c_access;
			bool find = map_.find(c_access, key);
			if (find) {
				// if (!c_access->second->weightedValue_->isAlive()) { return false; }
				if (c_access->second->weight_ == 0) { return false; }
				afterTask(const_cast<handle_base_type*>(c_access->second), OP::GET, thread_id);
				// result = c_access->second->weightedValue_->value_;
				result = c_access->second->value_;
			}
			return find;
		}

		bool GetQuietly(const KEY& key, VALUE& result) {
			const_accessor c_access;
			bool find = map_.find(c_access, key);
			if (find) {
				result = c_access->second->weightedValue_->value_;
			}
			return find;
		}

		void Print() {
			// assert(map_.size() >= in_use_list_size_);
			for (auto iter = map_.begin(); iter != map_.end(); iter++) {}
		}

		int ThreadNum() { return this->thread_num_; }
		rr::ConcurrentQueue<QueueNode>**& AsyncQueue() { return async_queue_; }
		std::mutex& in_use_list_mutex() { return in_use_list_mutex_; }
		tbb::concurrent_hash_map<KEY, handle_base_type*>& map() { return map_; }
		Manager*& manager() { return manager_; }

	private:
		/**
		 * @brief
		 *
		 * @param key
		 * @param value
		 * @param onlyIfAbsent
		 * @param prior 旧值的引用，当返回值false, prior 才能访问，否则未定义
		 * @param thread_id 认为的线程ID，为了 async_queue_ 减少竞争
		 * @return true 说明没有旧值，是新加入的
		 * @return false
		 */
		template<typename _Arg>
		bool put(const KEY& key, _Arg&& value, bool onlyIfAbsent, VALUE& prior, int thread_id) {
			if (in_use_list_size_.load() >= (max_size_ - 2)) {
				std::unique_lock<std::mutex> lck(in_use_list_mutex_);
				evict();
			}

			accessor acc;
			bool is_new = map_.insert(acc, key);
			if (is_new) {
				handle_base_type* handle = new handle_type(this, key, std::forward<_Arg>(value));
				assert(acc->second == nullptr);
				acc->second = handle;
				afterTask(acc->second, OP::PUT, thread_id);
				return is_new;
			}
			else if (onlyIfAbsent) {	// 仅读旧值，不写
				prior = acc->second->value_;
				return is_new;
			}
			// 需要更新 is_new==false, and onlyIfAbsent==false
			prior = acc->second->value_;
			acc->second->value_ = value;
			afterTask(acc->second, OP::PUT, thread_id);
			return is_new;
		}

		bool nodeInList(::list_node* node) {
			// 应该从头找，但是这样太费时，直接判断了前后是否为自己
			bool in = !(node == node->next && node == node->prev);
			if (in) {
				assert(node != NULL);
				assert(node->next);
				assert(node->prev);
			}
			return in;
		}

		// need lock in_use_list_mutex_ before call this
		// 头部表示最老的，尾部是最新的
		void use(QueueNode& node) {
						node.Print();
			cout << "use: " << __LINE__ << endl;
			if (!node.handle_exist_) { return; }
			cout << "use: " << __LINE__ << endl;
			handle_type* handle = (handle_type*)(node.handle_);
			if (node.op_ == OP::PUT) {
				evict();
				assert(node.handle_->weight_ > 0);
				if (node.handle_->weight_ > 0) {
					list_add_tail(&handle->node_, &in_use_list_);
					in_use_list_size_.fetch_add(1);
					cout << "in_use_list_size_: " << in_use_list_size_.load() << endl;
				}
				evict();
			}
			else if (node.op_ == OP::GET) {
				assert(node.handle_->weight_ > 0);
				if (node.handle_->weight_ > 0) {
					if (nodeInList(&handle->node_)) { list_move_tail(&handle->node_, &in_use_list_); }
				}
			}
			else if (node.op_ == OP::DEL) {
				assert(node.handle_->weight_ == 0);
				if (nodeInList(&handle->node_)) {
					list_del(&handle->node_);
					::list_node* tmp = &handle->node_;
					tmp->prev = tmp->next = tmp;
				}

				node.handle_exist_ = false;
				manager_->Delete(handle);
				delete node.handle_;
				node.handle_ = nullptr;
			}
		}


		// need lock in_use_list_mutex_ before call this
		void evict() {
			while (in_use_list_size_.load() >= max_size_) {
				::list_node* first = in_use_list_.next;
				if (first != &in_use_list_) {
					list_del(first);
					first->prev = first->next = first;
					in_use_list_size_.fetch_sub(1);
				}
				handle_type* handle = list_entry(first, handle_type, node_);

				accessor acc;
				bool find = map_.find(acc, handle->key_);
				if (find) {
					assert(acc->second == handle);
					handle->weight_ = 0;
					map_.erase(acc);
				}
				handle->weight_ = 0;
				handle->queueNode_->handle_exist_ = false;
				manager_->Delete(handle);
				delete handle;
				handle = nullptr;
			}
		}

		void afterTask(handle_base_type* handle, OP op, int thread_id) {
			QueueNode node(handle, op);
			// async_queue_[syscall(SYS_gettid) % thread_num_]->push(std::move(node));
			async_queue_[thread_id % thread_num_]->push(std::move(node));
			node.Print();
		}

	public:
		struct QueueNode {
		public:
			QueueNode(handle_base_type* handle, OP op) : handle_(handle), op_(op), handle_exist_(true) {
				((handle_type*)(handle_))->queueNode_ = this;
			};
			QueueNode& operator=(const QueueNode& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
				return *this;
			}
			QueueNode(QueueNode&& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
			}
			QueueNode(const QueueNode& node) {
				this->handle_ = node.handle_;
				this->op_ = node.op_;
			}
			QueueNode() {}
			~QueueNode() { /* no need free handle_*/ }


			handle_base_type* handle_;
			OP op_;
			/**
			 * @brief 多个线程指向同一个 handle_ 时，只要有一个线程 delete handle_，
			 * 其他线程不能用 if(handle_ == nullptr) 判断，只能通过 handle_exist_ 判断 handle_ 是否被释放
			 */
			bool handle_exist_;
			void Print() {
				cout << "handle_exist_: " << (handle_exist_ ? "true" : "false");
				if(handle_exist_) {
					cout << "OP: " << ConcurrentLinkedHashMap::OPToChar(op_) << ", key: " << handle_->key_ << ", value: " << (char*)handle_->value_ << endl;
				} else {
					cout << endl;
				}
			}
		};
	};
}

#endif // CONCURRENT_LINKED_HASH_MAP