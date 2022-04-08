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



static bool node_in_list(::list_node* node) {
	// 应该从头找，但是这样太费时，直接判断了前后是否为自己
	bool in = !(node == node->next && node == node->prev);
	if (in) {
		assert(node);
		assert(node->next);
		assert(node->prev);
	}
	return in;
}

namespace rr {
	// 原本 page 没有继承 Handle，需要 LRUCache 继承这个类，以执行元素被删除后的动作 
	class Manager_Hint {
	public:
		virtual void Delete(void* arg1) = 0;
		virtual void New(void* arg1) = 0;
	};

	// concurrent_map
	namespace concurrent_linked_map {
		// Manager 即 LRUCache 类，Manager_Hint 是因为前面的版本想要 LRUCache 继承 类，Manager_Hint
		template <typename KEY, typename VALUE, class Manager = Manager_Hint>
		class ConcurrentLinkedHashMap;
		class HandleBase {
		public:
			virtual ~HandleBase() {}

			HandleBase() {};
			// const HandleBase& operator=(const HandleBase& handle) = delete;
			// const HandleBase& operator=(HandleBase&& handle) = delete;
			// HandleBase(const HandleBase& handle) = delete;
			// HandleBase(HandleBase&& handle) = delete;
			atomic<size_t> op_time_{0};
		};

		template<typename KEY, typename VALUE, class Manager>
		class Handle : public HandleBase {
		public:
			template<typename K, typename V, typename M> friend class ConcurrentLinkedHashMap;
			typedef ConcurrentLinkedHashMap<KEY, VALUE, Manager> Map;
			typedef typename ConcurrentLinkedHashMap<KEY, VALUE, Manager>::QueueNode QueueNode;

		private:
			::list_node node_;
			std::atomic<size_t> weight_;
			std::atomic<bool> evicted_;	// evict 仅内部空间不够，被 lru 清除才调用
			std::atomic<bool> deleted_;	// 外部显式调用 Remove 才用到，后面异步删除掉 handle

		protected:
			KEY key_;
			VALUE value_;
			// internal_ref==0, 不代表 handle 就可以删除，ref 是 del handle 的必要条件（即ref==0，才能del；但是ref!=0，一定不能 del）
			std::atomic<size_t> internal_ref_;
			Map* mapParent_;

		public:
			VALUE& value() { return value_; }
			KEY& key() { return key_; }

			size_t InternalRef() { return internal_ref_.fetch_add(1); op_time_.fetch_add(1); }
			size_t InternalUnref() { return internal_ref_.fetch_sub(1); op_time_.fetch_add(1); }
			size_t InternalRefSize() { return internal_ref_.load(); }
			// 下面三个函数，被继承后，只要 Handle* 指向了子类，handle-> 调用时，都表现出子类的特性
			virtual size_t Ref() { return 0; }
			virtual size_t RefSize() { return 0; }
			virtual void RefClear() { internal_ref_.store(0); }

			bool Evicted() { return evicted_.load(); }
			bool Deleted() { return deleted_.load(); }

		public:
			void MakeDead() {
				size_t before = weight_.fetch_sub(1);
				assert(1 == before);
			}

			void MakeEvict() {
				assert(evicted_.load() == false);
				evicted_.store(true);
			}

			void MakeDelete() {
				assert(deleted_.load() == false);
				deleted_.store(true);
			}

			bool IsAlive() { return (weight_.load() > 0); }
			// bool IsDead() { return (weight_.load() <= 0); }
			bool IsDead() { return !IsAlive(); }


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

			void init() {
				INIT_LIST_HEAD(&node_);
				weight_.store(1);
				if (internal_ref_.load() != 0) {
					cout << __FILE__ << ", " << __LINE__ << ", internal_ref_: " << internal_ref_.load() << endl;
				}
				assert(internal_ref_.load() == 0);
				evicted_.store(false);
				deleted_.store(false);
			}

			virtual ~Handle() {
				node_.prev = nullptr;
				node_.next = nullptr;
				// no need free mapParent_ and queueNode_
				deleted_.store(true);
				evicted_.store(true);
			}

			// 由 LRUCache 继承，当前空间释放由子类决定，父类默认会释放当前 this 指针
			virtual void Delete();

			Handle(Map* map, const KEY& k, const VALUE& v) : mapParent_(map), key_(k), value_(v) { internal_ref_.store(0); init(); }

			Handle(Map* map, const KEY& k, VALUE&& v) : mapParent_(map), key_(k), value_(std::move(v)) { internal_ref_.store(0); init(); }

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
			Handle(Handle&& handle) = delete;
			Handle();
			// {
			// 	INIT_LIST_HEAD(&node);
			// 	this->mapParent_ = handle.mapParent_;
			// 	this->key = handle.key;
			// 	copy(&handle.node);
			// }
			// Handle() = delete;

		};

		template <typename KEY, typename VALUE, class Manager>
		class ConcurrentLinkedHashMap {
		public:
			friend class HandleBase;
			template <typename K, typename V, class M> friend class Handle;

			typedef KEY key_type;
			typedef VALUE value_type;

			/* or: */
			// typedef Handle<KEY, VALUE> Handle_type;
			// typedef tbb::concurrent_hash_map<KEY, Handle*> ConcurrentMap;
			// typedef typename ConcurrentMap::accessor accessor;
			// typedef typename ConcurrentMap::const_accessor const_accessor;
			using HandleBase = concurrent_linked_map::HandleBase;
			using handle_type = concurrent_linked_map::Handle<KEY, VALUE, Manager>;
			using ConcurrentMap = tbb::concurrent_hash_map<KEY, handle_type*>;
			using accessor = typename ConcurrentMap::accessor;
			using const_accessor = typename ConcurrentMap::const_accessor;


		public:
			struct QueueNode;

		private:
			Manager* manager_;
			bool should_stop_;	// 父线程/进程退出，此时不再需要后台删除
			bool has_shutdown_;	// 表示后台线程成功退出，可以正常析构，不然要等后台线程退出才能析构

			// 上层接口同步修改 map_，被删除的节点会放到异步驱逐队列 deleted_queue_, Handle* 被后台线程 delete；put 时 Handle* 被 new
			ConcurrentMap map_;
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
			::list_node in_use_list_;		// dummy_node_
			std::atomic<size_t> in_use_list_size_;
			std::mutex in_use_list_mutex_;

			// rr::ConcurrentQueue<handle_type*> deleted_queue_;
			::list_node deleted_queue_;		// dummy_node_
			std::atomic<size_t> deleted_size_;
			thread deleted_queue_thread_;


			void AsyncRWListThread() {
				int count = 0;

				while (!should_stop_) {
					for (auto i = 0; i < thread_num_; i++) {
						QueueNode front;
						bool res = async_queue_[i]->pop(front);
						if (res) {
							std::unique_lock<std::mutex> lck(in_use_list_mutex_);
							use(front);
						}
						else {
							// this_thread::sleep_for(chrono::milliseconds(1));
						}
					}
					count++;

					::list_node* first = deleted_queue_.next;
					while (1) {
						handle_type* handle = list_entry(first, handle_type, node_);
						if (first == &deleted_queue_) break;
						if (handle->InternalRefSize() <= 0 && handle->RefSize() <= 0) {
							if (!(handle->Evicted() || handle->Deleted())) {
								cout << __FILE__ << ", " << __LINE__
									<< ", Evicted: " << handle->Evicted()
									<< ", Deleted: " << handle->Deleted() << endl;
							}
							assert(handle->Evicted() || handle->Deleted());
							list_del(first);
							handle->Delete();
							deleted_size_.fetch_sub(1);
							break;
						}
						else {
							first = first->next;
						}
					}
				}
				has_shutdown_ = true;
				return;
			}

		public:
			ConcurrentLinkedHashMap() { ConcurrentLinkedHashMap(SIZE_MAX); };
			ConcurrentLinkedHashMap(size_t max_size) : in_use_list_size_(0), thread_num_(std::thread::hardware_concurrency())
				// , deleted_queue_(true)
				, should_stop_(false)
				, has_shutdown_(false)
				, manager_(nullptr) {
				INIT_LIST_HEAD(&in_use_list_);
				INIT_LIST_HEAD(&deleted_queue_);
				deleted_size_.store(0);

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
				// cout << "~ConcurrentLinkedHashMap()     , " << __FILE__ << ", " << __LINE__ << endl;
				wait_for_asyc_done();
				should_stop_ = true;

				// 需要等后台线程成功退出
				while (!has_shutdown_);
				for (auto i = 0; i < thread_num_; i++) {
					assert(async_queue_[i]->size() == 0);
					// QueueNode front;
					// while(async_queue_[i]->pop(front)) {
					// 	front.handle_->InternalUnref();
					// }
				}

				for (auto i = 0; i < thread_num_; i++) { async_queue_[i]->set_stop(); }
				for (auto i = 0; i < thread_num_; i++) { delete async_queue_[i]; }
				check();
				clear_map();

				// cout << "~ConcurrentLinkedHashMap() done, " << __FILE__ << ", " << __LINE__ << endl;
			}

			bool Put(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return         put(key, value, false, prior, thread_id); }
			bool Put(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return              put(key, std::move(value), false, prior, thread_id); }
			bool Put(const KEY& key, handle_type* handle, handle_type*& prior, int thread_id = 0) { return put(key, handle, false, prior, thread_id); }

			bool PutIfAbsent(const KEY& key, const VALUE& value, VALUE& prior, int thread_id = 0) { return put(key, value, true, prior, thread_id); }
			bool PutIfAbsent(const KEY& key, VALUE&& value, VALUE& prior, int thread_id = 0) { return      put(key, std::move(value), true, prior, thread_id); }
			bool PutIfAbsent(const KEY& key, handle_type* handle, handle_type*& prior, int thread_id = 0) { return put(key, handle, true, prior, thread_id); }

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
				handle_type* handle = nullptr;
				if (find) {
					handle = acc->second;
					bool deleted = map_.erase(acc);
					assert(deleted);

					prior = handle->value_;
					assert(handle->weight_.load() > 0);
					handle->MakeDead();
					afterTask(handle, OP::DEL, thread_id);
				}

				return find;
			}
			bool Remove(const KEY& key, handle_type*& prior, int thread_id = 0) {
				accessor acc;
				bool find = map_.find(acc, key);
				handle_type* handle = nullptr;
				if (find) {
					handle = acc->second;
					bool deleted = map_.erase(acc);
					assert(deleted);

					prior = handle;
					prior->Ref();
					assert(handle->weight_.load() > 0);
					handle->MakeDead();
					afterTask(handle, OP::DEL, thread_id);
				}

				return find;
			}

			bool Get(const KEY& key, VALUE& result, int thread_id = 0) {
				const_accessor c_access;
				bool find = map_.find(c_access, key);
				if (find) {
					assert(c_access->second->IsAlive());
					afterTask(const_cast<handle_type*>(c_access->second), OP::GET, thread_id);
					result = c_access->second->value_;
				}
				return find;
			}

			bool Get(const KEY& key, handle_type*& result, int thread_id = 0) {
				const_accessor c_access;
				bool find = map_.find(c_access, key);
				if (find) {
					assert(c_access->second->IsAlive());
					// 需要在外部显示调用 Unref()
					result = c_access->second;
					result->Ref();
					afterTask(const_cast<handle_type*>(c_access->second), OP::GET, thread_id);
				}
				return find;
			}

			bool GetQuietly(const KEY& key, VALUE& result) {
				const_accessor c_access;
				bool find = map_.find(c_access, key);
				if (find) {
					result = c_access->second->value_;
				}
				return find;
			}

			int ThreadNum() { return this->thread_num_; }
			rr::ConcurrentQueue<QueueNode>**& AsyncQueue() { return async_queue_; }
			std::mutex& in_use_list_mutex() { return in_use_list_mutex_; }
			tbb::concurrent_hash_map<KEY, handle_type*>& map() { return map_; }
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
				// cout << *(uint64_t*)(value) << ", " << (void*)value << ", " << __FILE__ << ", " << __LINE__ <<  endl; 

				accessor acc;
				bool is_new = map_.insert(acc, key);
				if (is_new) {
					handle_type* handle = new handle_type(this, key, std::forward<_Arg>(value));
					size_of_newhandle_.fetch_add(1);
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
				assert(acc->second->IsAlive());
				prior = acc->second->value_;
				acc->second->value_ = value;
				afterTask(acc->second, OP::GET, thread_id);
				return is_new;
			}


			bool put(const KEY& key, handle_type* handle, bool onlyIfAbsent, handle_type*& prior, int thread_id) {
				accessor acc;
				bool is_new = map_.insert(acc, key);
				size_of_newhandle_.fetch_add(1);	// 不能在 if(is_new) 里面，因为外部会调用 page->Delete(), 会减掉它
				if (is_new) {
					assert(acc->second == nullptr);
					acc->second = handle;
					handle->Ref();
					afterTask(acc->second, OP::PUT, thread_id);
					return is_new;
				}
				else if (onlyIfAbsent) {	// 仅读旧值，不写
					prior = acc->second;
					prior->Ref();
					return is_new;
				}
				// 需要更新: is_new==false, and onlyIfAbsent==false
				assert(acc->second->IsAlive());
				prior = acc->second;
				acc->second = handle;
				afterTask(acc->second, OP::GET, thread_id);
				return is_new;
			}

			// need lock in_use_list_mutex_ before call this
			// 头部表示最老的，尾部是最新的
			void use(QueueNode& node) {
				handle_type* handle = (handle_type*)node.handle_;

				size_t before = handle->InternalRefSize();
				{	// some check
					if (before < 1) {
						cout << "OP: " << OPToChar(node.op_) << endl;
						cout << __FILE__ << ", " << __LINE__ << ", ref: " << handle->InternalRefSize() << ", PageRef: " << handle->RefSize() << endl;
						cout << "handle op time: " << handle->op_time_ << endl;
					}
					assert(before >= 1);
				}
				if (node.op_ == OP::DEL) {
					assert(handle->IsDead());
					if (node_in_list(&handle->node_) && !handle->Evicted()) {
						list_del(&handle->node_);
						INIT_LIST_HEAD(&handle->node_);
						// before = handle->InternalUnref();	// 出链引用
						// assert(before >= 1);
						in_use_list_size_.fetch_sub(1);
					}
					else assert(handle->node_.prev == &handle->node_ && handle->node_.next == &handle->node_);
					assert(!node_in_list(&handle->node_));
					::list_add_tail(&handle->node_, &deleted_queue_);
					deleted_size_.fetch_add(1);
					handle->MakeDelete();
					before = handle->InternalUnref();
				}
				else if (node.op_ == OP::PUT) {
					if (handle->IsAlive() && !handle->deleted_.load() && !handle->evicted_.load()) {
					// if (handle->IsAlive()) {
						assert(!node_in_list(&handle->node_));
						list_add_tail(&handle->node_, &in_use_list_);
						// handle->InternalRef();				// 入链引用
						in_use_list_size_.fetch_add(1);
					}
					evict();
					before = handle->InternalUnref();
				}
				else if (node.op_ == OP::GET) {
					if (handle->IsAlive() && !handle->deleted_.load() && !handle->evicted_.load()) {
						if (node_in_list(&handle->node_)) { list_move_tail(&handle->node_, &in_use_list_); }
					}
					before = handle->InternalUnref();
				}
			}

			// need lock in_use_list_mutex_ before call this
			void evict() {
				::list_node* first = &in_use_list_;
				while (in_use_list_size_.load() >= max_size_) {
					first = first->next;
					handle_type* handle = list_entry(first, handle_type, node_);

					if (first == &in_use_list_) { return; }
					else if (handle->Deleted()) continue;
					else {
						assert(node_in_list(first));
						list_del(first);
						first->prev = first->next = first;
						in_use_list_size_.fetch_sub(1);

						accessor acc;
						bool find = map_.find(acc, handle->key_);
						// assert(find);	// 和 Remove 接口冲突
						if (find) {
							map_.erase(acc);
							assert(handle->weight_ > 0);
							handle->MakeDead();
							handle->evicted_.store(true);
							// 这里有 acc 锁，所以不会和前台线程的 handle->Ref() 冲突，保证能正确判断
							assert(!node_in_list(&handle->node_));
							::list_add_tail(first, &deleted_queue_);
							deleted_size_.fetch_add(1);
							// bool before = handle->InternalUnref();	// 出链引用
						}
					}
				}
			}

			void afterTask(handle_type* handle, OP op, int thread_id) {
				size_t before = handle->InternalRef(); assert(before >= 0);
				QueueNode node(handle, op);
				// if (before < 0) return;		// 可能被 evict
				// async_queue_[syscall(SYS_gettid) % thread_num_]->push(std::move(node));
				async_queue_[thread_id % thread_num_]->push(std::move(node));
			}


			void clear_map() {
				for (auto iter = map_.begin(); iter != map_.end(); iter++) {
					handle_type* handle = (handle_type*)iter->second;
					assert(handle->InternalRefSize() == 0);
					handle->RefClear();
					if (handle->RefSize() != 0) {
						cout << __FILE__ << ", " << __LINE__ << ", " << handle->RefSize() << endl;
					}
					assert(handle->RefSize() == 0);
					handle->Delete();
					size_of_newhandle_.fetch_sub(1);
					in_use_list_size_.fetch_sub(1);
				}
				INIT_LIST_HEAD(&in_use_list_);
				map_.clear();
			}

			static string OPToChar(OP op) {
				if (op == OP::PUT) { return string("OP::PUT"); }
				if (op == OP::GET) { return string("OP::GET"); }
				if (op == OP::DEL) { return string("OP::DEL"); }
				if (op == OP::UPDATE) { return string("OP::PUT"); }
				return string("NULL");
			}

		public:
			// for debug
			std::atomic<size_t> size_of_newhandle_{ 0 };

			void wait_for_asyc_done() {
				while (1) {
					bool done = true;
					for (int i = 0; i < ThreadNum(); i++) {
						if (async_queue_[i]->size() > 0) {
							done = false;
							// /*debug for print*/ cout << async_queue_[i]->size() << endl;
							// /*debug for print*/ std::this_thread::sleep_for(chrono::seconds(1));
						}
					}
					if (deleted_size_.load() > 0) { done = false; }
					if (done == true) return;
				}
			}

			struct QueueNode {
			public:
				QueueNode(handle_type* handle, OP op) : handle_(handle), op_(op) { };
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

				handle_type* handle_;
				OP op_;
				/**
				 * @brief 多个线程指向同一个 handle_ 时，只要有一个线程 delete handle_，
				 * 其他线程不能用 if(handle_ == nullptr) 判断，只能通过 handle_exist_ 判断 handle_ 是否被释放
				 */
				 // bool handle_exist_;
			};

			void check() {
				wait_for_asyc_done();
				std::this_thread::sleep_for(chrono::seconds(1));
				/*debug for print*/ std::cout << "map_size/new_handle_num/list_size: " << map_.size() << ", " << size_of_newhandle_.load() << ", " << in_use_list_size_.load() << std::endl;
				assert(map_.size() == in_use_list_size_.load());
				assert(map_.size() == size_of_newhandle_.load());

				for (::list_node* node = &in_use_list_; node->next != &in_use_list_; node = node->next) {
					/*debug for check*/assert(node->next->prev == node);
				}
			}

			void Print() {
				// assert(map_.size() >= in_use_list_size_);
				for (auto iter = map_.begin(); iter != map_.end(); iter++) {}
			}
		};

		template<typename KEY, typename VALUE, class Manager>
		void Handle<KEY, VALUE, Manager>::Delete() {
			INIT_LIST_HEAD(&node_);
			mapParent_->size_of_newhandle_.fetch_sub(1);
			delete this;
		}
	} // namespace concurrent_linked_map

	using concurrent_linked_map::ConcurrentLinkedHashMap;
}

#endif // CONCURRENT_LINKED_HASH_MAP