#ifndef CONCURRENT_LRU_CACHE
#define CONCURRENT_LRU_CACHE

#include<sys/mman.h>
#include "concurrent_linked_hash_map.h"
using namespace std;


namespace rr {
	// template<typename Allocator = std::allocator<void>>
	class LRUCache;

	using CMap = ConcurrentLinkedHashMap<uint64_t, void*>;
	using key_type = CMap::key_type;
	using value_type = CMap::value_type;
	using handle_base_type = con_map::HandleBase<uint64_t, void*, LRUCache>;
	using handle_type = con_map::Handle<uint64_t, void*, LRUCache>;



	namespace lru_cache {
		class Page {
		public:
			uint64_t key_;
			void* ptr_;
			LRUCache* lru_cache_;

			Page() : key_(UINT64_MAX), ptr_(nullptr), lru_cache_(nullptr) {}
			Page(uint64_t key, void* ptr) : key_(key), ptr_(ptr), lru_cache_(nullptr) {}
			Page(uint64_t key, void* ptr, LRUCache* lru_cache) : key_(key), ptr_(ptr), lru_cache_(lru_cache) {}
		};
		// class Page : public Handle<key_type, value_type> {
		// public:
		// 	Page() { lru_cache_ = nullptr; }
		// 	Page(rr::LRUCache* lru_cache, key_type key, value_type value)
		// 		: lru_cache_(lru_cache)
		// 		, Handle((CMap*)(lru_cache_), key, value)
		// 		// , HandleBase(lru_cache_, key, value)
		// 	{ }

		// 	virtual ~Page();

		// 	rr::LRUCache* lru_cache_;
		// };
	}

	class LRUCache : rr::Hint{
	public:
		using Page = lru_cache::Page;

		rr::ConcurrentQueue<Page> free_list_;
		std::atomic<size_t> free_list_size_{0};

		// max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_ 
		size_t max_bytes_;
		size_t item_bytes_;

		void* ptr_;

		ConcurrentLinkedHashMap<uint64_t, void*, LRUCache> cmap_;

	public:
		LRUCache(size_t max_bytes, size_t item_bytes) :
			cmap_(max_bytes / item_bytes)
			, max_bytes_(max_bytes)
			, item_bytes_(item_bytes)
			, free_list_(true)
		{
			free_list_size_.store(max_bytes / item_bytes);
			ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

			cmap_.manager() = this;

			std::unique_lock<std::mutex> lck(cmap_.in_use_list_mutex());
			char* cursor = (char*)ptr_;
			for (auto i = 0; i < free_list_size_; i++) {
				Page page(UINT64_MAX, cursor);
				cursor += item_bytes_;
				free_list_.push(std::move(page));
			}
		}

		bool Write(const key_type& key, const value_type ptr, int thread_id = 0) {
			value_type prior;
			return cmap_.PutIfAbsent(key, ptr, prior);
		}

		bool Read(const key_type& key, value_type ptr, int thread_id = 0) {
			return cmap_.Get(key, ptr);
		}

		bool GetNewPage(Page& page) {
			while (free_list_size_ <= 0) {}

			bool res = free_list_.pop(page);
			if (res) { free_list_size_.fetch_sub(1); }
			return res;
		}

		void Print() {
			cout << "map       size: " << cmap_.map().size() << endl;
			cout << "free list size: " << free_list_size_.load() << endl;

			size_t count_map = 0, count_list = 0;
			for (auto iter = cmap_.map().begin(); iter != cmap_.map().end(); iter++, count_map++) {
				cout << "key: " << iter->first << ", value: " << string((char*)iter->second->value(), item_bytes_) << endl;
			}

			cout << "free_list_     : " << endl;
			for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++, count_list++) {
				cout << "key: " << iter->key_ << ", value: " << string((char*)iter->ptr_, item_bytes_) << endl;
			}
			cout << "map       count: " << count_map << endl;
			cout << "free list count: " << count_list << endl;
		}

		virtual void Delete(void* handle) {
			handle_base_type* h = (handle_base_type*)handle;
			// assert(lru_cache == this);
			Page page(h->key(), h->value(), this);
			free_list_.emplace_push(page);
			free_list_size_.fetch_add(1);
			// std::cout << "recycle: " << ((char*)h->value()-(char*)ptr_) << endl;
		}
		virtual void New(void* handle) {}
	};



	/**
	// template<typename Allocator>
	class LRUCache : public CMap {
	public:
		friend class Page;

	protected:
		rr::ConcurrentQueue<Page> free_list_;
		std::atomic<size_t> free_list_size_;

		// max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_
		size_t max_bytes_;
		size_t item_bytes_;
		size_t item_num_;

		void* ptr_;

	public:
		LRUCache(size_t max_bytes, size_t item_bytes)
			// : CMap::ConcurrentLinkedHashMap(max_bytes / item_bytes)
			: ConcurrentLinkedHashMap(max_bytes / item_bytes)
			, max_bytes_(max_bytes)
			, item_bytes_(item_bytes)
			, item_num_(max_bytes / item_bytes)
			, free_list_(true)
		{
			ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

			std::unique_lock<std::mutex> lck(this->in_use_list_mutex());
			char* cursor = (char*)ptr_;
			for (auto i = 0; i < item_num_; i++) {
				Page page(this, UINT64_MAX, cursor);
				// page.key_ = UINT64_MAX;
				// page.ptr_ = cursor;
				cursor += item_bytes_;
				free_list_.push(std::move(page));
			}
		}

		bool Write(const key_type& key, const value_type ptr, int thread_id = 0) {
			value_type prior;
			return CMap::PutIfAbsent(key, ptr, prior);
		}

		bool Read(const key_type& key, value_type ptr, int thread_id = 0) {
			return CMap::Get(key, ptr);
		}

		bool GetNewPage(Page& page) {
			while (free_list_size_ <= 0) {}
			// assert(free_list_.pop(page));
			// return true;

			bool res = free_list_.pop(page);
			if (res) { free_list_size_.fetch_sub(1); }
			return res;
		}

		void Print() {
			for (auto iter = CMap::map_.begin(); iter != CMap::map_.end(); iter++) {
				cout << "key: " << iter->first << ", value: " << string((char*)iter->second->value_, item_bytes_) << endl;
			}

			for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++) {
				cout << "key: " << iter->key_ << ", value: " << string((char*)iter->value_, item_bytes_) << endl;
			}
		}

	};*/
}



#endif // CONCURRENT_LRU_CACHE