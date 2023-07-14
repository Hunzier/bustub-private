//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (!new_frame_.empty()) {
    frame_id_t find_id = -1;
    std::list<frame_id_t>::iterator find_it;
    for (auto it = new_frame_.begin(); it != new_frame_.end(); it++) {
      frame_id_t cur_id = (*it);
      auto &cur_node = node_store_[cur_id];
      if (cur_node.IsEvictable()) {
        if (find_id == -1 || cur_node.GetKthHistory() < node_store_[find_id].GetKthHistory()) {
          find_id = cur_id;
          find_it = it;
        }
      }
    }

    if (find_id != -1) {
      new_frame_.erase(find_it);
      node_store_.erase(find_id);
      (*frame_id) = find_id;
      curr_size_--;
      return true;
    }
  }

  if (!cache_frame_.empty()) {
    frame_id_t find_id = -1;
    std::list<frame_id_t>::iterator find_it;
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++) {
      frame_id_t cur_id = (*it);
      auto &cur_node = node_store_[cur_id];
      if (cur_node.IsEvictable()) {
        if (find_id == -1 || cur_node.GetKthHistory() < node_store_[find_id].GetKthHistory()) {
          find_id = cur_id;
          find_it = it;
        }
      }
    }

    if (find_id != -1) {
      cache_frame_.erase(find_it);
      node_store_.erase(find_id);
      (*frame_id) = find_id;
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }

  current_timestamp_++;

  // 如果已经有了
  if (node_store_.count(frame_id) != 0U) {
    auto &node = node_store_[frame_id];

    size_t size_of_history = node.Size();
    node.AddHistory(current_timestamp_);

    if (size_of_history + 1 == k_) {  // 转移
      // 删去旧的
      auto loc = new_locate_[frame_id];
      new_frame_.erase(loc);
      new_locate_.erase(frame_id);

      // 添加到cache中
      cache_frame_.push_front(frame_id);
      cache_locate_[frame_id] = cache_frame_.begin();
    } else if (size_of_history + 1 < k_ || k_ == 1) {  // 维护普通LRU
      // 删去旧的
      auto loc = new_locate_[frame_id];
      new_frame_.erase(loc);
      new_locate_.erase(frame_id);

      // 添加到new的队头
      new_frame_.push_front(frame_id);
      new_locate_[frame_id] = new_frame_.begin();
    }

  } else {  // 新建一个点
    if (curr_size_ == replacer_size_) {
      frame_id_t frame;
      Evict(&frame);
    }

    LRUKNode new_node(frame_id, k_);
    new_node.AddHistory(current_timestamp_);

    new_frame_.push_front(frame_id);
    new_locate_[frame_id] = new_frame_.begin();
    node_store_[frame_id] = new_node;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (node_store_.count(frame_id) == 0U) {
    return;
  }

  auto &node = node_store_[frame_id];

  bool state = node.IsEvictable();
  node.SetEvictable(set_evictable);
  node_store_[frame_id] = node;

  if (state != set_evictable) {
    if (!state) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0U) {
    return;
  }

  auto &node = node_store_[frame_id];

  if (!node.IsEvictable()) {
    throw std::exception();
  }

  if (node.Size() == k_ && k_ != 1) {
    auto loc = cache_locate_[frame_id];
    cache_frame_.erase(loc);
    cache_locate_.erase(frame_id);
  } else {
    auto loc = new_locate_[frame_id];
    new_frame_.erase(loc);
    new_locate_.erase(frame_id);
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
