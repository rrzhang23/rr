#ifndef CONCURRENT_LRU_CACHE
#define CONCURRENT_LRU_CACHE

#include<sys/mman.h>
#include "concurrent_linked_hash_map.h"
#include "config.h"
using namespace std;

// #define PAGE_SIZE (16 * 1024)
namespace rr {
	namespace lru_cache {
		class LRUCache;

		using CMap = rr::ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>;
		using key_type = CMap::key_type;
		using value_type = CMap::value_type;
		using handle_type = rr::concurrent_linked_map::Handle<uint64_t, void*, LRUCache>;

		// template<typename KEY, typename VALUE, class Manager>
		// class Page final : public con_map::Handle<KEY, VALUE, Manager> {
		class Page final : public handle_type {
		public:
			size_t size_;
			std::atomic<size_t> ref_;
			LRUCache* cache_;
			uint64_t used_size_;
			uint64_t version_;

			virtual size_t Ref() override { return ref_.fetch_add(1); }
			size_t Unref() { return ref_.fetch_sub(1); }
			virtual size_t RefSize() override { return ref_.load(); }
			virtual void RefClear() override { 
				this->internal_ref_.store(0); 
				// ref_.store(0); 
			}

			Page() :handle_type::Handle(nullptr, UINT64_MAX, nullptr), size_(0), cache_(nullptr) { ref_.store(0); }
			Page(LRUCache* cache) : handle_type::Handle(nullptr, UINT64_MAX, nullptr)
				, size_(0), cache_(cache) {
				ref_.store(0);
			}
			Page(uint64_t page_id, LRUCache* cache) : handle_type::Handle(nullptr, page_id, nullptr)
				, size_(0), cache_(cache) {
				ref_.store(0);
			}
			Page(uint64_t page_id, void* ptr, LRUCache* cache, uint64_t used_size = 3, uint64_t version = 0);

			virtual void Delete();

			~Page() {}

			void check() {
				if (value() == nullptr || value() == NULL) {
					assert(size_ == 0 && key() == UINT64_MAX);
				}
				else {
					assert(size_ == PAGE_SIZE);
				}
			}

			const Page& operator=(const Page& page) = delete;
			const Page& operator=(Page&& page) = delete;
			Page(const Page& page) = delete;
			Page(Page&& page) = delete;

			void reset() {
				handle_type::key_ = UINT64_MAX;
				handle_type::value_ = nullptr;
				size_ = 0;
			}

			void set(void* ptr) {
				set_key(UINT64_MAX);
				handle_type::value() = ptr;
				size_ = PAGE_SIZE;
				memcpy(handle_type::value_, (void*)(&(handle_type::key_)), sizeof(uint64_t));
			}

			void set(const uint64_t& page_id, void* ptr) {
				set_key(page_id);
				handle_type::value_ = ptr;
				size_ = PAGE_SIZE;
				memcpy(handle_type::value_, (void*)(&(handle_type::key_)), sizeof(uint64_t));
			}

			void set_key(const uint64_t& page_id) {
				handle_type::key_ = page_id;
			}

			void set_id(const uint64_t& id) {
				assert(handle_type::key_ == id);
				memcpy(handle_type::value_, (void*)(&id), sizeof(uint64_t));
			}

			void Print() {
				cout << "key: " << handle_type::key_ << ", ptr: " << *(uint64_t*)(handle_type::value_) << ", " << __FILE__ << ", " << __LINE__ << endl;
			}

			bool AddRow(void* row_date, size_t row_size) {
				if(used_size_ + row_size > PAGE_SIZE) {
					return false;
				} else {
					char* location = ((char*)handle_type::value_)+used_size_;
					memcpy(location, row_date, row_size);
					used_size_ += row_size;
					assert(used_size_ <= PAGE_SIZE);
					memcpy(((char*)handle_type::value_)+sizeof(uint64_t), &used_size_, sizeof(uint64_t));
					return true;
				}
			}
		};

		class LRUCache {
		public:
			rr::ConcurrentQueue<handle_type*> free_list_;
			std::atomic<size_t> free_list_size_{ 0 };

			// max_size_ in bytes, item_bytes_ in bytes, item_num_ = max_size_ / item_size_ 
			size_t max_bytes_;
			size_t item_bytes_;
			void* ptr_;

			template<typename K, typename V, class M> friend class rr::ConcurrentLinkedHashMap;
			ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>* cmap_;
			// TODO: remove // bool cmap_delete_done_;

		public:
			LRUCache(size_t max_bytes, size_t item_bytes) :
				// cmap_(max_bytes / item_bytes), 
				free_list_(true)
				, max_bytes_(max_bytes)
				, item_bytes_(item_bytes)
			{
				free_list_size_.store(max_bytes / item_bytes);
				// cout << "free_list_size_: " << free_list_size_ << ", max_bytes: " << max_bytes << ", item_bytes: " << item_bytes << endl;
				ptr_ = mmap(NULL, this->max_bytes_, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);

				cmap_ = new ConcurrentLinkedHashMap<uint64_t, void*, LRUCache>(max_bytes / item_bytes);
				cmap_->manager() = this;
				std::unique_lock<std::mutex> lck(cmap_->in_use_list_mutex());
				char* cursor = (char*)ptr_;
				for (size_t i = 0; i < free_list_size_; i++) {
					free_list_.push((handle_type*)(new Page(UINT64_MAX, cursor, this)));
					cursor += item_bytes_;
				}
				// TODO: remove // cmap_delete_done_ = false;
			}

			~LRUCache() {
    			// cout << "~LRUCache()     , " << __FILE__ << ", " << __LINE__ << endl;
				delete cmap_;
				
				handle_type* handle = nullptr;
				while(free_list_.pop(handle)) {
					assert(handle->RefSize() == 0 || handle->InternalRef() == 0);
					Page* page = (Page*) handle;
					delete page;
				}

				munmap(ptr_, this->max_bytes_);
    			// cout << "~LRUCache() done, " << __FILE__ << ", " << __LINE__ << endl;
			}

			bool Write(const key_type& key, Page* page, Page*&prior, int thread_id = 0) {
				assert(!prior);
				handle_type* prior_handle = nullptr;
				handle_type* handle = (handle_type*)page;
				page->set_key(key);
				bool res = cmap_->PutIfAbsent(key, handle, prior_handle, thread_id);
				if(res) {
					// cout << "ref: " << page->RefSize() << endl;
					// page->Unref();	// 外部 Unref
				} else {
					assert(prior_handle);
					prior = (Page*)prior_handle;
					// cout << "ref: " << prior->RefSize() << endl;
					// ((Page*)(prior))->Unref();	// 外部 Unref
					// handle->Delete(); 			// or:
					page->Delete();
				}
				return res;
			}

			bool Read(const key_type& key, Page*& page, int thread_id = 0) {
				handle_type* handle = nullptr;
				assert(!page);
				bool find = cmap_->Get(key, handle, thread_id);
				if (find) {
					assert(handle);
					page = (Page*)handle;
					// cout << __FILE__ << ", " << __LINE__ << ", ref: " << handle->RefSize() << endl;
					assert(page->value() != nullptr);
					assert(key == page->key());
				}
				return find;
			}

			bool Delete(const key_type& key, Page*& prior, int thread_id = 0) {
				handle_type* prior_handle = nullptr;
				bool res = cmap_->Remove(key, prior_handle, thread_id);
				if(res) {
					assert(prior_handle);
					prior = (Page*)prior_handle;
					assert(prior->value() != nullptr);
					assert(key == prior->key());
					// prior->Unref();	// 外部 Unref
				}
				return res;
			}

			bool GetNewPage(Page*& page) {
				handle_type* handle = nullptr;
				while (1) {
					if (free_list_size_ <= 0) continue;
					bool res = free_list_.pop(handle);
					if (res) {
						page = (Page*)handle;
						assert(page->RefSize() == 0 && page->InternalRefSize() == 0);
						free_list_size_.fetch_sub(1); assert(page);
						return res;
					}
				}
			}
		private:


		public:
			void Print() {
				cmap_->wait_for_asyc_done();
				cout << "map       size: " << cmap_->map().size() << endl;
				cout << "free list size: " << free_list_size_.load() << endl << endl;

				size_t count_map = 0, count_list = 0;
				cout << "buffer         : " << endl;
				for (auto iter = cmap_->map().begin(); iter != cmap_->map().end(); iter++, count_map++) {
					cout << "key: " << iter->first << ", value: " << *(uint64_t*)((handle_type*)iter->second->value()) << endl;
				}

				cout << "free_list_     : " << endl;
				for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++, count_list++) {
					cout << "key: " << (*iter)->key() << ", value: " << *(uint64_t*)((*iter)->value())
						<< ", addr: " << (*iter)->value() << endl;
				}
				cout << endl << "map       count: " << count_map << endl;
				cout << "free list count: " << count_list << endl;
			}

			void PrintRef() {
				sleep(3);
				size_t count_list = 0;
				for (auto iter = cmap_->map().begin(); iter != cmap_->map().end(); iter++) {
					cout << "int_ref: " << iter->second->InternalRefSize() << ", ref: " << iter->second->RefSize() << endl;
				}

				
				cout << "free_list_     : " << endl;
				for (auto iter = free_list_.begin(); iter != free_list_.end(); iter++, count_list++) {
					cout << "int_ref: " << (*iter)->InternalRefSize() << ", ref: " << (*iter)->RefSize() << endl;
				}
				cout << "PrintRef done, " << __FILE__ << ", " << __LINE__ << endl;
			}
		};
	}
	using namespace lru_cache;
}



#endif // CONCURRENT_LRU_CACHE