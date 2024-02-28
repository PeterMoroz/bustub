//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"


namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {

  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  const uint32_t hash = Hash(key);
  const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  const page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  BasicPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  const page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->emplace_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  const uint32_t hash = Hash(key);
  const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {

  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  BasicPageGuard directory_guard;
  if (directory_page_id == INVALID_PAGE_ID) {
    directory_guard = bpm_->NewPageGuarded(&directory_page_id);
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
  } else {
    directory_guard = bpm_->FetchPageBasic(directory_page_id);
  }

  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);

  return InsertToNewBucket(directory_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {

  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  BasicPageGuard bucket_guard;
  if (bucket_page_id == INVALID_PAGE_ID) {
    bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    // directory->SetBucketPageId(bucket_idx, bucket_page_id);
    // directory->SetLocalDepth(bucket_idx, 0);
    UpdateDirectoryMapping(directory, bucket_idx, bucket_page_id, 0, 0 /* ignored*/);
  } else {
    bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
  }

  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->Insert(key, value, cmp_)) {
    return true;
  }

  // insert failed because the bucket is full.
  if (bucket_page->IsFull()) {
    if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
      if (directory->GetGlobalDepth() == directory->GetMaxDepth()) {
        return false;
      }
      directory->IncrGlobalDepth(); 
    }

    page_id_t split_page_id = INVALID_PAGE_ID;
    BasicPageGuard split_guard = bpm_->NewPageGuarded(&split_page_id);
    auto split_page = split_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    split_page->Init(bucket_max_size_);

    directory->IncrLocalDepth(bucket_idx);
    const uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
    UpdateDirectoryMapping(directory, split_idx, split_page_id, directory->GetLocalDepth(bucket_idx), 0 /* ignored*/);

    uint32_t i = 0;
    while (i < bucket_page->Size()) {
      auto entry = bucket_page->EntryAt(i);
      if (directory->HashToBucketIndex(Hash(entry.first)) == split_idx) {
        BUSTUB_ASSERT(split_page->Insert(entry.first, entry.second, cmp_), "insert into split bucket failed");
        bucket_page->RemoveAt(i);
        continue;
      }
      i++;      
    }    

    const uint32_t hash = Hash(key);
    bucket_idx = directory->HashToBucketIndex(hash);
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  BUSTUB_ASSERT(false, "should be unreachable.");
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  const uint32_t hash = Hash(key);
  const uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  const page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  BasicPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  const uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  const page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }

  if (bucket_page->IsEmpty()) {
    if (directory_page->GetGlobalDepth() > 0) {
      const uint32_t split_idx = directory_page->GetSplitImageIndex(bucket_idx);
      directory_page->DecrLocalDepth(split_idx);
      const page_id_t page_id = directory_page->GetBucketPageId(split_idx);    
      const uint32_t local_depth = directory_page->GetLocalDepth(split_idx);
      UpdateDirectoryMapping(directory_page, bucket_idx, page_id, local_depth, 0 /* ignored */);
      if (directory_page->CanShrink()) {
        directory_page->DecrGlobalDepth();
      }
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
