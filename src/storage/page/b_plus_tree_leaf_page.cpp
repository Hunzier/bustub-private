//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // 如果没有值, 直接加
  if (GetSize() == 0) {
    IncreaseSize(1);
    array_[0] = {key, value};
    return true;
  }

  // 如果比最大的还大, 直接加后面
  if (comparator(KeyAt(GetSize() - 1), key) < 0) {
    IncreaseSize(1);
    array_[GetSize() - 1] = {key, value};
    return true;
  }

  int l = 0;
  int r = GetSize() - 1;

  // 找第一个大于等于key的
  while (l < r) {
    int mid = (l + r) >> 1;
    auto compare = comparator(KeyAt(mid), key);

    // 相同返回false
    if (compare == 0) {
      return false;
    }

    // 如果Key[mid] < key
    if (compare < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

  // key == Key[l]
  if (comparator(KeyAt(l), key) == 0) {
    return false;
  }

  IncreaseSize(1);
  for (int i = GetSize() - 1; i > l; i--) {
    array_[i] = array_[i - 1];
  }
  array_[l] = {key, value};
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (GetSize() == 0) {
    return false;
  }

  int l = 0;
  int r = GetSize() - 1;

  if (comparator(KeyAt(0), key) == 0) {
    l = 0;
  } else if (comparator(KeyAt(r), key) == 0) {
    l = r;
  } else {
    // 找第一个大于等于key的
    while (l < r) {
      int mid = (l + r) >> 1;
      auto compare = comparator(KeyAt(mid), key);

      // 如果Key[mid] < key
      if (compare < 0) {
        l = mid + 1;
      } else {
        r = mid;
      }
    }

    // key == Key[l]
    if (comparator(KeyAt(l), key) != 0) {
      return false;
    }
  }

  IncreaseSize(-1);
  for (int i = l; i < GetSize(); ++i) {
    array_[i] = array_[i + 1];
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int index) -> bool {
  if (index >= GetSize()) {
    return false;
  }

  IncreaseSize(-1);
  for (int i = index; i < GetSize(); ++i) {
    array_[i] = array_[i + 1];
  }
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
