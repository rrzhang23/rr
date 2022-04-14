#include "concurrent_lru_cache.h"
namespace rr {
    namespace lru_cache {
        void Page::Delete() {
            handle_type* h = (handle_type*)this;
            // assert(this->ref_.load() == 0);
            assert(h->RefSize() == 0 && h->InternalRefSize() == 0);
            this->set_key(UINT64_MAX);
            this->set_id(UINT64_MAX);
            uint64_t used_size = 3;
            memcpy(((char*)h->value())+sizeof(uint64_t), &used_size, sizeof(uint64_t));
            memcpy(&used_size_, ((char*)h->value())+sizeof(uint64_t), sizeof(uint64_t));
            assert(this->used_size_ == 3);
            this->init();
            cache_->free_list_.emplace_push(h);
            cache_->free_list_size_.fetch_add(1);
            cache_->cmap_->size_of_newhandle_.fetch_sub(1);
        }

        Page::Page(uint64_t page_id, void* ptr, LRUCache* cache, uint64_t used_size, uint64_t version) : handle_type::Handle(cache->cmap_, page_id, ptr)
            , size_(PAGE_SIZE), cache_(cache), used_size_(used_size), version_(version) {
            ref_.store(0);
        }
    }
}