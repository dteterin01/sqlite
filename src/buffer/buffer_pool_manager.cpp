#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager *disk_manager,
                                     LogManager *log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager),
          log_manager_(log_manager) {
    // a consecutive memory space for buffer pool
    pages_ = new Page[pool_size_];
    page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
    replacer_ = new LRUReplacer<Page *>;
    free_list_ = new std::list<Page *>;

    // put all the pages into free list
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_->push_back(&pages_[i]);
    }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
    delete[] pages_;
    delete page_table_;
    delete replacer_;
    delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    Page * page;
    if (page_table_->Find(page_id, page)) {
        page->pin_count_++;
        replacer_->Erase(page);
        return page;
    }

    page = SelectFreePage();
    if (page == nullptr) {
        return nullptr;
    }

    page->page_id_ = page_id;
    page->pin_count_++;
    disk_manager_->ReadPage(page_id, page->GetData());
    page_table_->Insert(page_id, page);

    return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    Page * page;
    if (!page_table_->Find(page_id, page) || page->pin_count_ <= 0) {
        return false;
    }

    if (--page->pin_count_ == 0) replacer_->Insert(page);
    if (is_dirty) page->is_dirty_ = is_dirty;
    return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    Page * page;
    if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, page)) {
        return false;
    }

    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;

    return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    Page * page;
    if (!page_table_->Find(page_id, page) || page->pin_count_ != 0) {
        return false;
    }

    page_table_->Remove(page_id);
    replacer_->Erase(page);

    page->page_id_ = INVALID_PAGE_ID;
    page->ResetMemory();
    page->is_dirty_ = false;

    disk_manager_->DeallocatePage(page_id);
    free_list_->push_back(page);

    return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    std::lock_guard<std::mutex> guard(latch_);

    Page* page = SelectFreePage();
    if (page == nullptr) {
        return nullptr;
    }

    page_id = disk_manager_->AllocatePage();

    page->page_id_ = page_id;
    page->ResetMemory();
    page->pin_count_++;

    page_table_->Insert(page_id, page);
    return page;
}

Page *BufferPoolManager::SelectFreePage() {
    if (!free_list_->empty()) {
        Page * page = free_list_->front();
        free_list_->pop_front();
        return page;
    }

    Page* page = nullptr;
    if (replacer_->Victim(page)) {
        if (page->is_dirty_) {
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->is_dirty_ = false;
        }

        page_table_->Remove(page->page_id_);
    }

    return page;
}
} // namespace cmudb
