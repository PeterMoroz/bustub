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
#include "common/exception.h"

#include <algorithm>
#include <iterator>

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

void LRUKNode::RecordAccess(size_t timestamp) { history_.push_back(timestamp); }

size_t LRUKNode::GetKDistance(size_t curr_timestamp) const {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  const size_t kth_timestamp = *std::prev(history_.cend(), k_);
  return curr_timestamp - kth_timestamp;
}

size_t LRUKNode::GetLeastRecentAccessTime() const {
  BUSTUB_ASSERT(!history_.empty(), "the frame access history is empty");
  return history_.back();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  size_t inf_count = 0;
  frame_id_t victim_id = std::numeric_limits<frame_id_t>::max();
  size_t max_kdistance = 0;
  auto it = node_store_.begin();
  while (it != node_store_.end()) {
    if (it->second.GetEvictable()) {
      const size_t kdistance = it->second.GetKDistance(current_timestamp_);
      if (kdistance > max_kdistance) {
        max_kdistance = kdistance;
        victim_id = it->first;
      }

      if (kdistance == std::numeric_limits<size_t>::max()) {
        inf_count++;
      }
    }
    ++it;
  }

  if (victim_id == std::numeric_limits<frame_id_t>::max()) {
    return false;
  }

  if (inf_count > 1) {
    return EvictLRU(frame_id);
  }

  node_store_.erase(victim_id);
  *frame_id = victim_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "invalid frame_id");
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.cend()) {
    it = node_store_.insert(std::pair(frame_id, LRUKNode(k_, frame_id))).first;
  }
  it->second.RecordAccess(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "invalid frame_id");
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  BUSTUB_ASSERT(it != node_store_.cend(), "there is no frame to mark as evicatable in the store");
  it->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it != node_store_.cend()) {
    BUSTUB_ASSERT(it->second.GetEvictable(), "unable to remove non-evictable frame");
    node_store_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return std::count_if(node_store_.cbegin(), node_store_.cend(),
                       [](const auto &item) { return item.second.GetEvictable(); });
}

auto LRUKReplacer::EvictLRU(frame_id_t *frame_id) -> bool {
  frame_id_t victim_id = std::numeric_limits<frame_id_t>::max();
  size_t min_time = std::numeric_limits<size_t>::max();
  auto it = node_store_.begin();
  while (it != node_store_.end()) {
    if (it->second.GetEvictable()) {
      const size_t least_recent_access_time = it->second.GetLeastRecentAccessTime();
      if (least_recent_access_time < min_time) {
        min_time = least_recent_access_time;
        victim_id = it->first;
      }
    }
    ++it;
  }

  if (victim_id != std::numeric_limits<frame_id_t>::max()) {
    node_store_.erase(victim_id);
    *frame_id = victim_id;
    return true;
  }
  return false;
}

}  // namespace bustub
