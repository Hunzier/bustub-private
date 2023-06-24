#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if(root_ == nullptr) {
    return nullptr;
  }

  auto curNode = root_;
  for (auto & v : key) {
    if(curNode->children_.count(v) == 0) {
      return nullptr;
    }
    curNode = curNode->children_.at(v);
  }

  if(curNode->is_value_node_ == false) {
    return nullptr;
  }

  const TrieNodeWithValue<T>* curValueNode = dynamic_cast<const TrieNodeWithValue<T>* >(curNode.get());

  return curValueNode->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  std::shared_ptr<TrieNode> cur{nullptr};
  std::shared_ptr<TrieNode> root{nullptr};

  if(root_ == nullptr) {
    root = std::make_shared<TrieNode>();
    cur = root;
  } else {
    root = std::shared_ptr<TrieNode>(root_->Clone());
    cur = root;
  }

  for (int i = 0; i < (int)key.size() - 1; i ++ ) {
    char v = key[i];
    std::shared_ptr<TrieNode> ne{nullptr};
    if(cur->children_.count(v) == 0) {
      ne = std::make_shared<TrieNode>();
    } else {
      ne = std::shared_ptr<TrieNode>(cur->children_[v]->Clone());
    }
    cur->children_[v] = ne;
    cur = ne;
  }

  if(key.size()) {
    char v = key.back();
    std::shared_ptr<TrieNodeWithValue<T>> ne{nullptr};

    if(cur->children_.count(v) == 0) {
      ne = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      ne = std::make_shared<TrieNodeWithValue<T>>(cur->children_[v]->children_, std::make_shared<T>(std::move(value)));
    }
    cur->children_[v] = ne;
  }

  return std::move(Trie(root));
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if(root_ == nullptr) {
    return Trie(nullptr);
  }

  std::shared_ptr<TrieNode> cur(nullptr);
  std::vector<std::shared_ptr<TrieNode>> nodes;

  auto root = std::shared_ptr<TrieNode>(root_->Clone());

  if(key.size() == 0) {
    return Trie(root);
  }

  cur = root;
  nodes.push_back(cur);
  bool can_find = true;
  for (int i = 0; i < (int)key.size() - 1; i ++ ) {
    auto &v = key[i];
    std::shared_ptr<TrieNode> ne{nullptr};

    if(cur->children_.count(v) == 0) {
      can_find = false;
      break;
    }

    ne = std::shared_ptr<TrieNode>(cur->children_[v]->Clone());
    cur = ne;
    nodes.push_back(cur);
  }

  //最后一个节点, 去掉Value
  if(cur->children_.count(key.back()) == 0) {
    can_find = false;
  }

  if(can_find == true) {
    auto &v = key.back();
    std::shared_ptr<TrieNode> ne = nullptr;
    ne = std::make_shared<TrieNode>(cur->children_[v]->children_);
    cur = ne;
    nodes.push_back(cur);
  }

  bool flag = false;
  for (int i = (int)nodes.size() - 1; i >= 0; i -- ) { // 删节点
    auto &t = nodes[i];
    if(i != (int)nodes.size() - 1) {
      t->children_.erase(key[i]);
    }

    if(flag) {
      t->children_[key[i]] = nodes[i + 1];
    }

    if(t->children_.size() == 0) {
      flag = false;
    } else {
      flag = true;
    }
  }
  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
