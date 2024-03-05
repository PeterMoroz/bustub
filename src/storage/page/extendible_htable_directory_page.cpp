//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (size_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  static const uint32_t masks[] = {0x00000000, 0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
                                   0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff, 0x00001fff,
                                   0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff, 0x0007ffff, 0x000fffff,
                                   0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff,
                                   0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff};
  return global_depth_ < 32 ? hash & masks[global_depth_] : hash & masks[32];
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  const uint8_t depth = local_depths_[bucket_idx];
  return bucket_idx ^ (1 << (depth - 1));
}

// auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
//   const uint8_t depth = local_depths_[bucket_idx];
//   return (1 << depth) - 1;
// }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // global_depth_++;
  // const uint32_t size = Size();
  // if (size == 2) {
  //   local_depths_[1] = local_depths_[0];
  //   bucket_page_ids_[1] = bucket_page_ids_[0];
  // } else {
  //   uint32_t i = 0;
  //   uint32_t j = size >> 1;
  //   for (; j < size; i++, j++) {
  //     local_depths_[j] = local_depths_[i];
  //     bucket_page_ids_[j] = bucket_page_ids_[i];
  //   }
  // }
  BUSTUB_ASSERT(global_depth_ < max_depth_, "global depth has reached the maximal value");
  const uint32_t origin_size = Size();
  uint32_t idx = origin_size;
  uint32_t origin_idx = 0;
  for (; origin_idx < origin_size; origin_idx++, idx++) {
    local_depths_[idx] = local_depths_[origin_idx];
    bucket_page_ids_[idx] = bucket_page_ids_[origin_idx];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // const uint32_t size = Size();
  // for (uint32_t i = 0; i < size; i++) {
  //   if (local_depths_[i] == global_depth_) {
  //     return false;
  //   }
  // }
  // return true;

  const uint32_t size = Size();
  if (size < 2) {
    return false;
  }
  const uint32_t half_size = size >> 1;
  uint32_t i = 0, j = half_size;
  for (; i < half_size && j < size; i++, j++) {
    if ((local_depths_[i] != local_depths_[j]) || (bucket_page_ids_[i] != bucket_page_ids_[j])) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "bucket_idx is out of range");
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
