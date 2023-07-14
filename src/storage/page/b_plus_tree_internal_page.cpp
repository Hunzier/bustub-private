//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  if (GetSize() == 0) {
    IncreaseSize(1);
    array_[0] = {key, value};
    return true;
  }

  if (comparator(KeyAt(GetSize() - 1), key) < 0) {
    IncreaseSize(1);
    array_[GetSize() - 1] = {key, value};
    return true;
  }

  int l = 0;
  int r = GetSize() - 1;

  // 第一个大于key的
  while (l < r) {
    int mid = (l + r) >> 1;
    auto compare = comparator(KeyAt(mid), key);
    if (compare == 0) {
      return false;
    }

    if (compare < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (value == ValueAt(i)) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  if (comparator(KeyAt(GetSize() - 1), key) < 0) {
    return GetSize();
  }
  int l = 0;
  int r = GetSize() - 1;
  // 第一个大于等于key的
  while (l < r) {
    int mid = (l + r) >> 1;
    auto compare = comparator(KeyAt(mid), key);
    if (compare < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) -> bool {
  if (index >= GetSize()) {
    return false;
  }

  IncreaseSize(-1);
  for (int i = index; i < GetSize(); ++i) {
    array_[i] = array_[i + 1];
  }

  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
