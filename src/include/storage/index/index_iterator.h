//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <memory>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index) {
    bpm_ = bpm;
    if (page_id == INVALID_PAGE_ID) {
      return;
    }
    index_ = index;
    guard_ = std::make_unique<BasicPageGuard>(bpm_->FetchPageBasic(page_id));
    page_ = const_cast<LeafPage *>(guard_->As<LeafPage>());
  }

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return itr.index_ == index_ && itr.page_ == page_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return itr.index_ != index_ || itr.page_ != page_; }

 private:
  // add your own private member variables here
  int index_{0};  // Page中的位置
  BufferPoolManager *bpm_{nullptr};
  std::shared_ptr<BasicPageGuard> guard_{nullptr};
  LeafPage *page_{nullptr};
  MappingType mapping_;
};

}  // namespace bustub
