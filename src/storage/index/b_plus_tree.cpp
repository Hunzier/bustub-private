
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  bpm_->NewPageGuarded(&(header_page->root_page_id_));
  auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto leaf_root_page = root_guard.AsMut<LeafPage>();
  leaf_root_page->Init(leaf_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (bpm_ == nullptr) {
    throw ExecutionException("BPlusTree::IsEmpty: bpm_ == nullptr.");
  }

  if (header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  auto root_page = bpm_->FetchPageRead(header_page->root_page_id_).As<BPlusTreePage>();
  return root_page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  auto opt = FindLeafRead(key, &ctx);
  if (opt.has_value()) {
    auto &guard = ctx.read_set_.back();
    auto page = guard.As<LeafPage>();
    auto index = opt.value().second;
    result->push_back(page->ValueAt(index));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafRead(const KeyType &key, Context *ctx) -> std::optional<std::pair<page_id_t, int>> {
  // Declaration of context instance.
  if (IsEmpty()) {
    return std::nullopt;
  }

  ctx->root_page_id_ = GetRootPageId();
  auto root_guard = bpm_->FetchPageRead(ctx->root_page_id_);
  ctx->read_set_.push_back(std::move(root_guard));

  while (true) {
    auto &now_guard = ctx->read_set_.back();

    // Is LeafPage
    if (now_guard.As<BPlusTreePage>()->IsLeafPage()) {
      auto current_page = now_guard.As<LeafPage>();

      int l = 0;
      int r = current_page->GetSize() - 1;
      while (l < r) {
        int mid = (l + r) >> 1;
        if (comparator_(current_page->KeyAt(mid), key) >= 0) {
          r = mid;
        } else {
          l = mid + 1;
        }
      }

      if (static_cast<bool>(comparator_(current_page->KeyAt(l), key) == 0)) {
        return std::make_pair(ctx->root_page_id_, l);
      }
      return std::nullopt;
    }

    // Isn't LeafPage
    auto current_page = now_guard.As<InternalPage>();

    int l = 1;
    int r = current_page->GetSize() - 1;
    if (current_page->GetSize() == 1 || comparator_(current_page->KeyAt(1), key) > 0) {
      l = 0;
    } else {
      while (l < r) {
        int mid = (l + r + 1) >> 1;
        if (comparator_(current_page->KeyAt(mid), key) > 0) {
          r = mid - 1;
        } else {
          l = mid;
        }
      }
    }
    auto next_page_id = current_page->ValueAt(l);
    ctx->root_page_id_ = next_page_id;
    auto next_page = bpm_->FetchPageRead(next_page_id);
    ctx->read_set_.push_back(std::move(next_page));
  }

  ctx->Clear();
  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafWrite(const KeyType &key, Context *ctx) -> std::optional<int> {
  // Declaration of context instance.
  if (IsEmpty()) {
    return std::optional<int>{std::nullopt};
  }

  ctx->header_page_ = bpm_->FetchPageWrite(header_page_id_);
  ctx->root_page_id_ = ctx->header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  auto root_guard = bpm_->FetchPageWrite(ctx->root_page_id_);
  ctx->write_set_.push_back(std::move(root_guard));

  while (true) {
    auto &now_guard = ctx->write_set_.back();

    // Is LeafPage
    if (now_guard.As<BPlusTreePage>()->IsLeafPage()) {
      auto current_page = now_guard.As<LeafPage>();

      int l = 0;
      int r = current_page->GetSize() - 1;
      while (l < r) {
        int mid = (l + r) >> 1;
        if (comparator_(current_page->KeyAt(mid), key) >= 0) {
          r = mid;
        } else {
          l = mid + 1;
        }
      }
      return static_cast<bool>(comparator_(current_page->KeyAt(l), key) == 0) ? std::optional<int>{l}
                                                                              : std::optional<int>{std::nullopt};
    }

    // Isn't LeafPage
    auto current_page = now_guard.As<InternalPage>();

    int l = 1;
    int r = current_page->GetSize() - 1;
    if (current_page->GetSize() == 1 || comparator_(current_page->KeyAt(1), key) > 0) {
      l = 0;
    } else {
      while (l < r) {
        int mid = (l + r + 1) >> 1;
        if (comparator_(current_page->KeyAt(mid), key) > 0) {
          r = mid - 1;
        } else {
          l = mid;
        }
      }
    }
    auto next_page_id = current_page->ValueAt(l);
    auto next_page = bpm_->FetchPageWrite(next_page_id);
    ctx->write_set_.push_back(std::move(next_page));
  }

  ctx->Clear();
  return std::optional<int>{std::nullopt};
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DfsInsert(page_id_t &root_page_id, const KeyType &key, const ValueType &value, bool &splited,
                               page_id_t &new_page_id, KeyType &pre_key, Transaction *txn) -> bool {
  auto root_guard = bpm_->FetchPageWrite(root_page_id);
  auto root_page = root_guard.As<BPlusTreePage>();

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_guard.AsMut<LeafPage>();
    pre_key = leaf_page->KeyAt(0);
    if (leaf_page->Insert(key, value, comparator_)) {
      if (leaf_page->GetSize() != leaf_page->GetMaxSize()) {
        splited = false;
        return true;
      }
      splited = true;
      auto get_new_guard = bpm_->NewPageGuarded(&new_page_id);
      auto new_guard = bpm_->FetchPageWrite(new_page_id);
      auto new_page = new_guard.AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);
      new_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(new_page_id);
      while (leaf_page->GetSize() > leaf_page->GetMinSize()) {
        int size = leaf_page->GetSize();
        auto k = leaf_page->KeyAt(size - 1);
        auto v = leaf_page->ValueAt(size - 1);
        new_page->Insert(k, v, comparator_);
        leaf_page->IncreaseSize(-1);
      }
      return true;
    }
    splited = false;
    return false;
  }

  auto internal_page = root_guard.AsMut<InternalPage>();
  KeyType get_pre_key = internal_page->KeyAt(0);

  int l = 1;
  int r = internal_page->GetSize() - 1;
  int index = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    auto compare = comparator_(key, internal_page->KeyAt(mid));
    if (compare >= 0) {
      index = mid;
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }

  page_id_t son_page_id = internal_page->ValueAt(index);
  if (!DfsInsert(son_page_id, key, value, splited, new_page_id, pre_key, txn)) {
    return false;
  }

  internal_page->Remove(pre_key, comparator_);
  auto son_guard = bpm_->FetchPageWrite(son_page_id);
  auto son_page = son_guard.AsMut<BPlusTreePage>();
  KeyType k;
  if (son_page->IsLeafPage()) {
    auto son_leaf_page = son_guard.As<LeafPage>();
    k = son_leaf_page->KeyAt(0);
  } else {
    auto son_internal_page = son_guard.As<InternalPage>();
    k = son_internal_page->KeyAt(0);
  }
  internal_page->Insert(k, son_page_id, comparator_);

  if (!splited) {
    pre_key = get_pre_key;
    return true;
  }

  auto insert_guard = bpm_->FetchPageWrite(new_page_id);
  auto insert_page = insert_guard.AsMut<BPlusTreePage>();
  page_id_t v = new_page_id;
  if (insert_page->IsLeafPage()) {
    auto insert_leaf_page = insert_guard.As<LeafPage>();
    k = insert_leaf_page->KeyAt(0);
  } else {
    auto insert_internal_page = insert_guard.As<InternalPage>();
    k = insert_internal_page->KeyAt(0);
  }

  if (internal_page->GetSize() != internal_page->GetMaxSize()) {
    splited = false;
    internal_page->Insert(k, v, comparator_);
    pre_key = get_pre_key;
    return true;
  }

  auto get_new_guard = bpm_->NewPageGuarded(&new_page_id);
  auto new_guard = bpm_->FetchPageWrite(new_page_id);
  auto new_page = new_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);
  if (comparator_(k, internal_page->KeyAt(internal_page->GetSize() - 1)) > 0) {
    new_page->Insert(k, v, comparator_);
  } else {
    int size = internal_page->GetSize();
    new_page->Insert(internal_page->KeyAt(size - 1), internal_page->ValueAt(size - 1), comparator_);
    internal_page->IncreaseSize(-1);
    internal_page->Insert(k, v, comparator_);
  }
  while (internal_page->GetSize() > internal_page->GetMinSize()) {
    int size = internal_page->GetSize();
    auto k = internal_page->KeyAt(size - 1);
    auto v = internal_page->ValueAt(size - 1);
    new_page->Insert(k, v, comparator_);
    internal_page->IncreaseSize(-1);
  }
  splited = true;
  pre_key = get_pre_key;

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  // std::cout << key.ToString() << "\n";
  // if (IsEmpty()) {
  //   StartNewTree(key, value);
  //   return true;
  // }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  bool splited = false;
  page_id_t new_page_id = INVALID_PAGE_ID;
  KeyType pre_key;
  if (DfsInsert(header_page->root_page_id_, key, value, splited, new_page_id, pre_key, txn)) {
    if (!splited) {
      return true;
    }

    page_id_t new_root_page_id = INVALID_PAGE_ID;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_page_id);
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);

    auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
    auto root_page = root_guard.As<BPlusTreePage>();
    if (root_page->IsLeafPage()) {
      auto leaf_page = root_guard.As<LeafPage>();
      auto k = leaf_page->KeyAt(0);
      new_root_page->Insert(k, header_page->root_page_id_, comparator_);

      auto new_guard = bpm_->FetchPageWrite(new_page_id);
      auto new_page = new_guard.As<LeafPage>();
      k = new_page->KeyAt(0);
      new_root_page->Insert(k, new_page_id, comparator_);
    } else {
      auto internal_page = root_guard.As<InternalPage>();
      auto k = internal_page->KeyAt(0);
      new_root_page->Insert(k, header_page->root_page_id_, comparator_);

      auto new_guard = bpm_->FetchPageWrite(new_page_id);
      auto new_page = new_guard.As<InternalPage>();
      k = new_page->KeyAt(0);
      new_root_page->Insert(k, new_page_id, comparator_);
    }

    header_page->root_page_id_ = new_root_page_id;
    return true;
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DfsRemove(page_id_t &root_page_id, const page_id_t &left_son_page_id,
                               const page_id_t &right_son_page_id, const KeyType &key,
                               std::vector<std::pair<KeyType, page_id_t>> &delete_node,
                               std::vector<std::pair<KeyType, page_id_t>> &add_node, Transaction *txn) {
  auto root_guard = bpm_->FetchPageWrite(root_page_id);
  auto root_page = root_guard.As<BPlusTreePage>();

  if (root_page->IsLeafPage()) {
    delete_node.clear();
    add_node.clear();
    auto leaf_page = root_guard.AsMut<LeafPage>();
    delete_node.push_back({leaf_page->KeyAt(0), root_page_id});
    leaf_page->Remove(key, comparator_);
    if (leaf_page->GetSize() >= leaf_page->GetMinSize() ||
        (left_son_page_id == INVALID_PAGE_ID && right_son_page_id == INVALID_PAGE_ID)) {
      add_node.push_back({leaf_page->KeyAt(0), root_page_id});
      return;
    }

    if (left_son_page_id != INVALID_PAGE_ID) {
      auto left_son_guard = bpm_->FetchPageWrite(left_son_page_id);
      auto left_son_leaf_page = left_son_guard.AsMut<LeafPage>();
      if (left_son_leaf_page->GetSize() - 1 >= left_son_leaf_page->GetMinSize()) {
        int size = left_son_leaf_page->GetSize();
        leaf_page->Insert(left_son_leaf_page->KeyAt(size - 1), left_son_leaf_page->ValueAt(size - 1), comparator_);
        left_son_leaf_page->IncreaseSize(-1);
        add_node.push_back({leaf_page->KeyAt(0), root_page_id});
        return;
      }
    }
    if (right_son_page_id != INVALID_PAGE_ID) {
      auto right_son_guard = bpm_->FetchPageWrite(right_son_page_id);
      auto right_son_leaf_page = right_son_guard.AsMut<LeafPage>();
      if (right_son_leaf_page->GetSize() - 1 >= right_son_leaf_page->GetMinSize()) {
        delete_node.push_back({right_son_leaf_page->KeyAt(0), right_son_page_id});
        leaf_page->Insert(right_son_leaf_page->KeyAt(0), right_son_leaf_page->ValueAt(0), comparator_);
        right_son_leaf_page->Remove(right_son_leaf_page->KeyAt(0), comparator_);
        add_node.push_back({leaf_page->KeyAt(0), root_page_id});
        add_node.push_back({right_son_leaf_page->KeyAt(0), right_son_page_id});
        return;
      }
    }

    if (left_son_page_id != INVALID_PAGE_ID) {
      auto left_son_guard = bpm_->FetchPageWrite(left_son_page_id);
      auto left_son_leaf_page = left_son_guard.AsMut<LeafPage>();
      delete_node.push_back({left_son_leaf_page->KeyAt(0), left_son_page_id});
      while (leaf_page->GetSize() > 0) {
        int size = leaf_page->GetSize();
        auto k = leaf_page->KeyAt(size - 1);
        auto v = leaf_page->ValueAt(size - 1);
        left_son_leaf_page->Insert(k, v, comparator_);
        leaf_page->IncreaseSize(-1);
      }
      add_node.push_back({left_son_leaf_page->KeyAt(0), left_son_page_id});
      left_son_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
      return;
    }
    if (right_son_page_id != INVALID_PAGE_ID) {
      auto right_son_guard = bpm_->FetchPageWrite(right_son_page_id);
      auto right_son_leaf_page = right_son_guard.AsMut<LeafPage>();
      delete_node.push_back({right_son_leaf_page->KeyAt(0), right_son_page_id});
      while (right_son_leaf_page->GetSize() > 0) {
        int size = right_son_leaf_page->GetSize();
        auto k = right_son_leaf_page->KeyAt(size - 1);
        auto v = right_son_leaf_page->ValueAt(size - 1);
        leaf_page->Insert(k, v, comparator_);
        right_son_leaf_page->IncreaseSize(-1);
      }
      add_node.push_back({leaf_page->KeyAt(0), root_page_id});
      leaf_page->SetNextPageId(right_son_leaf_page->GetNextPageId());
      return;
    }

    return;
  }

  auto internal_page = root_guard.AsMut<InternalPage>();

  int l = 1;
  int r = internal_page->GetSize() - 1;
  int index = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    auto compare = comparator_(key, internal_page->KeyAt(mid));
    if (compare >= 0) {
      index = mid;
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }

  page_id_t new_left_son_page_id = INVALID_PAGE_ID;
  page_id_t new_right_son_page_id = INVALID_PAGE_ID;
  page_id_t son_page_id = internal_page->ValueAt(index);
  if (index - 1 >= 0) {
    new_left_son_page_id = internal_page->ValueAt(index - 1);
  }
  if (index + 1 < internal_page->GetSize()) {
    new_right_son_page_id = internal_page->ValueAt(index + 1);
  }
  DfsRemove(son_page_id, new_left_son_page_id, new_right_son_page_id, key, delete_node, add_node, txn);
  auto pre_key = internal_page->KeyAt(0);

  for (auto [k, v] : delete_node) {
    internal_page->Remove(k, comparator_);
  }
  for (auto [k, v] : add_node) {
    internal_page->Insert(k, v, comparator_);
  }
  delete_node.clear();
  add_node.clear();
  delete_node.push_back({pre_key, root_page_id});

  if (internal_page->GetSize() >= internal_page->GetMinSize() ||
      (left_son_page_id == INVALID_PAGE_ID && right_son_page_id == INVALID_PAGE_ID)) {
    add_node.push_back({internal_page->KeyAt(0), root_page_id});
    return;
  }

  if (left_son_page_id != INVALID_PAGE_ID) {
    auto left_son_guard = bpm_->FetchPageWrite(left_son_page_id);
    auto left_son_internal_page = left_son_guard.AsMut<InternalPage>();
    if (left_son_internal_page->GetSize() - 1 >= left_son_internal_page->GetMinSize()) {
      int size = left_son_internal_page->GetSize();
      auto k = left_son_internal_page->KeyAt(size - 1);
      auto v = left_son_internal_page->ValueAt(size - 1);
      internal_page->Insert(k, v, comparator_);
      left_son_internal_page->IncreaseSize(-1);
      add_node.push_back({internal_page->KeyAt(0), root_page_id});
      return;
    }
  }
  if (right_son_page_id != INVALID_PAGE_ID) {
    auto right_son_guard = bpm_->FetchPageWrite(right_son_page_id);
    auto right_son_internal_page = right_son_guard.AsMut<InternalPage>();
    if (right_son_internal_page->GetSize() - 1 >= right_son_internal_page->GetMinSize()) {
      delete_node.push_back({right_son_internal_page->KeyAt(0), right_son_page_id});
      internal_page->Insert(right_son_internal_page->KeyAt(0), right_son_internal_page->ValueAt(0), comparator_);
      right_son_internal_page->Remove(right_son_internal_page->KeyAt(0), comparator_);
      add_node.push_back({internal_page->KeyAt(0), root_page_id});
      add_node.push_back({right_son_internal_page->KeyAt(0), right_son_page_id});
      return;
    }
  }

  if (left_son_page_id != INVALID_PAGE_ID) {
    auto left_son_guard = bpm_->FetchPageWrite(left_son_page_id);
    auto left_son_internal_page = left_son_guard.AsMut<InternalPage>();
    delete_node.push_back({left_son_internal_page->KeyAt(0), left_son_page_id});
    while (left_son_internal_page->GetSize() > 0) {
      int size = left_son_internal_page->GetSize();
      auto k = left_son_internal_page->KeyAt(size - 1);
      auto v = left_son_internal_page->ValueAt(size - 1);
      internal_page->Insert(k, v, comparator_);
      left_son_internal_page->IncreaseSize(-1);
    }
    add_node.push_back({internal_page->KeyAt(0), root_page_id});
    return;
  }
  if (right_son_page_id != INVALID_PAGE_ID) {
    auto right_son_guard = bpm_->FetchPageWrite(right_son_page_id);
    auto right_son_internal_page = right_son_guard.AsMut<InternalPage>();
    delete_node.push_back({right_son_internal_page->KeyAt(0), right_son_page_id});
    while (right_son_internal_page->GetSize() > 0) {
      int size = right_son_internal_page->GetSize();
      auto k = right_son_internal_page->KeyAt(size - 1);
      auto v = right_son_internal_page->ValueAt(size - 1);
      internal_page->Insert(k, v, comparator_);
      right_son_internal_page->IncreaseSize(-1);
    }
    add_node.push_back({internal_page->KeyAt(0), root_page_id});
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  if (IsEmpty()) {
    return;
  }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  std::vector<std::pair<KeyType, page_id_t>> delete_node;
  std::vector<std::pair<KeyType, page_id_t>> add_node;
  DfsRemove(header_page->root_page_id_, INVALID_PAGE_ID, INVALID_PAGE_ID, key, delete_node, add_node, txn);
  auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  if (!root_page->IsLeafPage() && root_page->GetSize() == 1) {
    auto internal_page = root_guard.As<InternalPage>();
    header_page->root_page_id_ = internal_page->ValueAt(0);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  page_id_t find_page_id = INVALID_PAGE_ID;
  {
    page_id_t root_page_id = GetRootPageId();
    auto root_guard = bpm_->FetchPageRead(root_page_id);
    if (root_guard.As<BPlusTreePage>()->IsLeafPage()) {
      return INDEXITERATOR_TYPE(bpm_, root_page_id, 0);
    }
    auto root_page = root_guard.As<InternalPage>();
    auto find_key = root_page->KeyAt(0);

    Context ctx;
    auto opt = FindLeafRead(find_key, &ctx);
    if (opt.has_value()) {
      find_page_id = opt.value().first;
    }
  }

  return INDEXITERATOR_TYPE(bpm_, find_page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  Context ctx;
  auto opt = FindLeafRead(key, &ctx);

  if (!opt.has_value()) {
    return End();
  }

  return INDEXITERATOR_TYPE(bpm_, opt.value().first, opt.value().second);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
