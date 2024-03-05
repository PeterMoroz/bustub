//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    Page *page = &pages_[frame_id];
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->ResetMemory();
    return page;
  }

  if (replacer_->Evict(&frame_id)) {
    Page *page = &pages_[frame_id];
    if (page->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
      if (!future.get()) {
        return nullptr;
      }
    }

    page_id_t tmp_page_id = INVALID_PAGE_ID;
    for (const auto &[key, value] : page_table_) {
      if (value == frame_id) {
        tmp_page_id = key;
        break;
      }
    }
    if (tmp_page_id != INVALID_PAGE_ID) {
      page_table_.erase(tmp_page_id);
    }

    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->ResetMemory();
    return page;
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  auto it = page_table_.find(page_id);
  if (it != page_table_.cend()) {
    frame_id = it->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    // page->page_id_ = page_id;
    BUSTUB_ASSERT(page->page_id_ == page_id, " mismatch of page id ");
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  } else if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_id;
  } else {
    if (replacer_->Evict(&frame_id)) {
      Page *page = &pages_[frame_id];
      if (page->is_dirty_) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
        BUSTUB_ENSURE(future.get(), "the page write request could not complete");
      }

      page_id_t tmp_page_id = INVALID_PAGE_ID;
      for (const auto &[key, value] : page_table_) {
        if (value == frame_id) {
          tmp_page_id = key;
          break;
        }
      }
      if (tmp_page_id != INVALID_PAGE_ID) {
        page_table_.erase(tmp_page_id);
      }

      page_table_[page_id] = frame_id;
    } else {
      return nullptr;
    }
  }

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  char data[BUSTUB_PAGE_SIZE] = {0};
  disk_scheduler_->Schedule({false, data, page_id, std::move(promise)});
  BUSTUB_ENSURE(future.get(), "the page read request could not complete");

  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < pool_size_, "the frame id is too large");
  Page *page = &pages_[frame_id];
  page->pin_count_ = 1;
  page->page_id_ = page_id;
  std::memcpy(page->data_, data, BUSTUB_PAGE_SIZE);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.cend()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    page->pin_count_--;
    page->is_dirty_ = is_dirty;
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.cend()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  BUSTUB_ASSERT(page->page_id_ != INVALID_PAGE_ID, "invalid page id");
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
  BUSTUB_ENSURE(future.get(), "the page write request could not complete");
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (const auto &[page_id, frame_id] : page_table_) {
    Page *page = &pages_[frame_id];
    BUSTUB_ASSERT(page->page_id_ == page_id, "mismatch of page ids");
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->data_, page->page_id_, std::move(promise)});
    BUSTUB_ASSERT(future.get(), "the page write request could not complete");
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.cend()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->pin_count_ == 0) {
    // what about dirty flag ?
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
  }
  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
