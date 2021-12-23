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

#define DEFAULT_MAX_SIZE INT64_MAX

extern int process_state;

template <typename KEY, typename VALUE>
class ConcurrentLinkedHashMap;

template <typename KEY, typename VALUE>
class ConcurrentLinkedHashMap {
public:
	struct LRUHandle;
	friend LRUHandle;
	// class WeightedValue;
	struct QueueNode;

	typedef std::pair<const KEY, VALUE> value_type;
	using ConcurrentMap = tbb::concurrent_hash_map<KEY, LRUHandle*>;
	using accessor = typename ConcurrentMap::accessor;
	using const_accessor = typename ConcurrentMap::const_accessor;

	// LRUHandle* deleted;


private:
	// 上层接口同步修改 map_，被删除的节点会放到异步驱逐队列 deleteDQueue_, LRUHandle* 被后台线程 delete；put 时 LRUHandle* 被 new
	tbb::concurrent_hash_map<KEY, LRUHandle*> map_;
	size_t maxSize_{ DEFAULT_MAX_SIZE };

	/**
	 * @brief 修改完 map_ 后，修改链表 unusedList_ 异步操作，先把操作记录进 asyncQueue_，在慢慢合并至 unusedList_。
	 * asyncQueue_ 是个数组 asyncQueue_[CPU_num]，避免多线程访问同一个 ConcurrentQueue 竞争
	 * asyncQueueMutex_ 很少用
	 */
	enum class OP { PUT, GET, UPDATE, DEL };
	rr::ConcurrentQueue<QueueNode>** asyncQueue_;
	size_t thread_num_;
	std::mutex asyncQueueMutex_;
	thread asyncQueueThread_;

	/**
	 * @brief 先进先出列表，read 操作会把已经在里面的取到最后面，write 会直接追加至后面，从前面 pop 删除
	 * unusedListMutex_ 用于保护这几个成员
	 */
	list_node unusedList_;
	std::atomic<size_t> unusedSize_;
	std::mutex unusedListMutex_;

	rr::ConcurrentQueue<LRUHandle*> deleteDQueue_;
	std::atomic<size_t> deleteDSize_;
	thread deleteDQueueThread_;


	void AsyncRWListThread() {
		int count = 0;

		while (process_state) {
			for (auto i = 0; i < thread_num_; i++) {
				if (!asyncQueue_[i]->empty()) {
					QueueNode front;
					bool res = asyncQueue_[i]->pop(front);
					if(res) {
						std::unique_lock<std::mutex> lck(unusedListMutex_);
						use(front);
					}
				}
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
	// 		LRUHandle* tmp = deleteDQueue_.pop();
	// 		if (tmp != nullptr) {
	// 			assert(!tmp->weightedValue_->isAlive());
	// 			delete tmp;
	// 			tmp = nullptr;
	// 		}
	// 	}
	// 	return;
	// }

public:

	ConcurrentLinkedHashMap() = delete;
	ConcurrentLinkedHashMap(int max_size) : unusedSize_(0), thread_num_(std::thread::hardware_concurrency())
		, deleteDQueue_(true) {
		INIT_LIST_HEAD(&unusedList_);
		assert(unusedSize_ <= maxSize_);
		maxSize_ = max_size;

		asyncQueue_ = new rr::ConcurrentQueue<QueueNode>*[thread_num_];
		for (auto i = 0; i < thread_num_; i++) {
			asyncQueue_[i] = new rr::ConcurrentQueue<QueueNode>(true);
			assert(asyncQueue_[i] != nullptr);
			// assert(asyncQueue_[i]->head_.load() != nullptr);
		}
		std::cout << thread_num_ << " cores" << std::endl;

		asyncQueueThread_ = thread(&ConcurrentLinkedHashMap::AsyncRWListThread, this);
		asyncQueueThread_.detach();

		// deleteDQueueThread_ = thread(&ConcurrentLinkedHashMap::AsyncDeleteDQueueThread, this);
		// deleteDQueueThread_.detach();
	}

	bool Put(const KEY& key, VALUE value, VALUE& prior, int thread_id = 0) { return put(key, value, false, prior, thread_id); }
	bool PutIfAbsent(const KEY& key, VALUE value, VALUE& prior, int thread_id = 0) { return put(key, value, true, prior, thread_id); }

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
		LRUHandle* handle = nullptr;
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
			afterTask(const_cast<LRUHandle*>(c_access->second), OP::GET, thread_id);
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
		// assert(map_.size() >= unusedSize_);
		for (auto iter = map_.begin(); iter != map_.end(); iter++) {
			// std::cout << iter->first << " " << iter->second->weightedValue_->value_ << std::endl;
		}
	}

	int ThreadNum() { return this->thread_num_; }

	rr::ConcurrentQueue<QueueNode>**& AsyncQueue() { return asyncQueue_; }
private:
	/**
	 * @brief
	 *
	 * @param key
	 * @param value
	 * @param onlyIfAbsent
	 * @param prior 旧值的引用，当返回值false, prior 才能访问，否则未定义
	 * @param thread_id 认为的线程ID，为了 asyncQueue_ 减少竞争
	 * @return true 说明没有旧值，是新加入的
	 * @return false
	 */
	bool put(const KEY& key, VALUE value, bool onlyIfAbsent, VALUE& prior, int thread_id) {
		// WeightedValue* weightedValue = new WeightedValue(value);

		accessor acc;
		bool is_new = map_.insert(acc, key);
		if (is_new) {
			LRUHandle* handle = new LRUHandle(this, key, value);
			assert(acc->second == nullptr);
			acc->second = handle;
			afterTask(acc->second, OP::PUT, thread_id);
			return is_new;
		}
		else if (onlyIfAbsent) {	// 仅读旧值，不写
			// prior = acc->second->weightedValue_->value_;
			prior = acc->second->value_;
		}
		// 需要更新 is_new==false, and onlyIfAbsent==false
		prior = acc->second->value_;
		acc->second->value_ = value;
		afterTask(acc->second, OP::PUT, thread_id);
		return is_new;
	}

	bool nodeInList(list_node* node) {
		// 应该从头找，但是这样太费时，直接判断了前后是否为自己
		bool in = !(node == node->next && node == node->prev);
		if (in) {
			assert(node != NULL);
			assert(node->next);
			assert(node->prev);
		}
		return in;
	}

	// need lock unusedListMutex_ before call this
	// 头部表示最老的，尾部是最新的
	void use(QueueNode& node) {
		if (node.handle_exist_ == false) { return; }
		if (node.op_ == OP::PUT) {
			evict();
			assert(node.handle_->weight_ > 0);
			if (node.handle_->weight_ > 0) {
				list_add_tail(&node.handle_->node_, &unusedList_);
				unusedSize_.fetch_add(1);
			}
		}
		else if (node.op_ == OP::GET) {
			assert(node.handle_->weight_ > 0);
			if (node.handle_->weight_ > 0) {
				if (nodeInList(&node.handle_->node_)) { list_move_tail(&node.handle_->node_, &unusedList_); }
			}
		}
		else if (node.op_ == OP::DEL) {
			assert(node.handle_->weight_ == 0);
			if (nodeInList(&node.handle_->node_)) {
				list_del(&node.handle_->node_);
				list_node* tmp = &node.handle_->node_;
				tmp->prev = tmp->next = tmp;
			}
			// deleteDQueue_.push(node.handle_);
			node.handle_exist_ = false;
			delete node.handle_;
			node.handle_ = nullptr;
		}
	}


	// need lock unusedListMutex_ before call this
	void evict() {
		while (unusedSize_.load() >= maxSize_) {
			list_node* first = unusedList_.next;
			if (first != &unusedList_) {
				list_del(first);
				first->prev = first->next = first;
				unusedSize_.fetch_sub(1);
			}
			LRUHandle* handle = list_entry(first, LRUHandle, node_);
			
			accessor acc;
			bool find = map_.find(acc, handle->key_);
			if (find) {
				assert(acc->second == handle);
				handle->weight_ = 0;
				map_.erase(acc);
			}
			handle->weight_ = 0;	
			handle->queueNode_.handle_exist_ = false;
			delete handle;
			handle = nullptr;
		}
	}

	void afterTask(LRUHandle* handle, OP op, int thread_id) {
		QueueNode node(handle, op);
		// asyncQueue_[syscall(SYS_gettid) % thread_num_]->push(std::move(node));
		asyncQueue_[thread_id % thread_num_]->push(std::move(node));
	}

public:
	struct QueueNode {
	public:
		QueueNode(LRUHandle* handle, OP op) : handle_(handle), op_(op), handle_exist_(true) {};
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

		
		LRUHandle* handle_;
		OP op_;
		/**
		 * @brief 多个线程指向同一个 handle_ 时，只要有一个线程 delete handle_，
		 * 其他线程不能用 if(handle_ == nullptr) 判断，只能通过 handle_exist_ 判断 handle_， 是否被释放
		 */
		bool handle_exist_;
	};

	// class WeightedValue {
	// 	friend class ConcurrentLinkedHashMap<KEY, VALUE>;

	// public:
	// 	VALUE value_;
	// 	int weight_;
	// 	WeightedValue(VALUE v, int weight) : value_(v), weight_(weight) {}
	// 	WeightedValue(VALUE v) : value_(v), weight_(1) {}
	// 	WeightedValue() {}
	// 	~WeightedValue() { /* no need free value_*/ }

	// 	bool contains(VALUE value) {}
	// 	bool isAlive() { return weight_ > 0; }
	// 	bool isRetired() { return weight_ < 0; }
	// 	bool isDead() { return weight_ == 0; }
	// };
};

template <typename KEY, typename VALUE>
struct ConcurrentLinkedHashMap<KEY, VALUE>::LRUHandle {
	friend class ConcurrentLinkedHashMap<KEY, VALUE>;

public:
	// std::atomic<WeightedValue*> weightedValue_{ nullptr };
	// WeightedValue* weightedValue_{ nullptr };
	// VALUE  value_;

public:
	~LRUHandle() {
		node_.prev = nullptr;
		node_.next = nullptr;
		// no need free mapParent_
	}

	LRUHandle(ConcurrentLinkedHashMap* map, const KEY& k, VALUE value) : mapParent_(map), key_(k), value_(value), weight_(1) {
		INIT_LIST_HEAD(&node_);
	}

	// void copy(list_node* node) {
	// 	if(node->next == node) {
	// 		assert(node->prev == node);
	// 		INIT_LIST_HEAD(node);
	// 	} else {
	// 		list_node* prev = node->prev;
	// 		list_node* next = node->next;
	// 		this->node.prev = prev;
	// 		prev->next = this->node;
	// 		this->node.next = next;
	// 		next->prev = this->node;
	// 	}
	// }
	const LRUHandle& operator=(const LRUHandle& handle) = delete;
	// {
	// 	INIT_LIST_HEAD(&node);
	// 	this->mapParent_ = handle.mapParent_;
	// 	this->key = handle.key;
	// 	this->in_cache = handle.in_cache;
	// 	copy(&handle.node);
	// 	return *this;
	// }
	LRUHandle(const LRUHandle& handle) = delete;
	// {
	// 	INIT_LIST_HEAD(&node);
	// 	this->mapParent_ = handle.mapParent_;
	// 	this->key = handle.key;
	// 	this->in_cache = handle.in_cache;
	// 	copy(&handle.node);
	// }
	LRUHandle() = delete;
	// {
	// 	INIT_LIST_HEAD(&node);
	// }


private:
	ConcurrentLinkedHashMap* mapParent_;
	KEY key_;
	VALUE value_;
	list_node node_;
	QueueNode queueNode_;
	// bool in_cache_;
	int weight_;

	friend ConcurrentLinkedHashMap;
};