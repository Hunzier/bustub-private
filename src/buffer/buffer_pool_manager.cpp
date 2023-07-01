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
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

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
  std::scoped_lock<std::mutex> lock(latch_);
  bool pinall = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      pinall = false;
      break;
    }
  }
  if (pinall) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();

    (*page_id) = AllocatePage();
    page_table_[(*page_id)] = free_frame_id;
    pages_[free_frame_id].page_id_ = (*page_id);
    pages_[free_frame_id].pin_count_ = 1;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    return &pages_[free_frame_id];
  }

  frame_id_t fid;
  replacer_->Evict(&fid);
  auto evicted_page_id = pages_[fid].GetPageId();

  // 清理
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(evicted_page_id, pages_[fid].GetData());
    pages_[fid].is_dirty_ = false;
  }
  pages_[fid].ResetMemory();
  page_table_.erase(evicted_page_id);

  // 重新加
  (*page_id) = AllocatePage();
  page_table_[(*page_id)] = fid;
  pages_[fid].page_id_ = (*page_id);
  pages_[fid].pin_count_ = 1;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type)
    -> Page * {  // 获取page_id对应的page
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_.count(page_id) != 0U) {
    fid = page_table_[page_id];

    pages_[fid].pin_count_++;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return &pages_[fid];
  }
  bool pinall = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      pinall = false;
      break;
    }
  }
  if (pinall) {
    return nullptr;
  }
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&fid);
    page_id_t evicted_page_id = pages_[fid].GetPageId();
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
    pages_[fid].ResetMemory();
    page_table_.erase(evicted_page_id);
  }
  page_table_[page_id] = fid;
  pages_[fid].page_id_ = page_id;
  pages_[fid].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[fid].GetData());
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return false;
  }

  auto fid = page_table_[page_id];
  if (pages_[fid].pin_count_ <= 0) {
    return false;
  }

  pages_[fid].is_dirty_ |= is_dirty;

  pages_[fid].pin_count_--;
  if (pages_[fid].pin_count_ <= 0) {
    replacer_->SetEvictable(fid, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0U || page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto fid = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPage(pages_[i].GetPageId());
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }

  auto fid = page_table_[page_id];
  if (pages_[fid].pin_count_ > 0) {
    return false;
  }

  replacer_->Remove(fid);
  free_list_.push_back(fid);
  pages_[fid].ResetMemory();
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 0;
  pages_[fid].page_id_ = INVALID_PAGE_ID;

  page_table_.erase(page_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);

  if (page == nullptr) {
    return {this, nullptr};
  }
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  return {this, page};
}

}  // namespace bustub
