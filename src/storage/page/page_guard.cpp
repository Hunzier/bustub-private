#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this->bpm_ != nullptr) {
    Drop();
  }

  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 检查自赋值情况
  if (this == &that) {
    return *this;
  }

  // 释放当前持有的页面资源
  Drop();

  // 将新页面的指针和对应的缓冲池管理器指针赋值给当前对象
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  // 将原对象的页面指针和缓冲池管理器指针置为空
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  Drop();
  this->guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // 检查自赋值情况
  if (this == &that) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  Drop();
  this->guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // 检查自赋值情况
  if (this == &that) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
