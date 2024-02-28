//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  size_ = 0;
  max_size_ = max_size;
  for (size_t i = 0; i < max_size; i++) {
    array_[i] = {};
  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  if (size_ != 0) {
    uint32_t pos = size_;
    if (bsearch(key, cmp, &pos)) {
      BUSTUB_ASSERT(pos < size_, "position is out of bound");
      value = array_[pos].second;
      return true;
    }
    return false;
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (size_ < max_size_) {
    if (size_ == 0) {
      array_[0] = std::make_pair(key, value);
      size_++;
      return true;      
    }

    if (cmp(key, array_[0].first) == 0) {
      array_[0].second = value;
      return true;
    }
    if (cmp(key, array_[0].first) == -1) {
      uint32_t idx = size_;
      while (idx > 0) {
        array_[idx] = std::move(array_[idx - 1]);
        idx--;
      }
      array_[0] = std::make_pair(key, value);
      size_++;
      return true;
    }

    if (cmp(key, array_[size_ - 1].first) == 0) {
      array_[size_ - 1].second = value;
      return true;
    }
    if (cmp(key, array_[size_ - 1].first) == 1) {
      array_[size_] = std::make_pair(key, value);
      size_++;
      return true;
    }

    uint32_t pos = size_;
    if (bsearch(key, cmp, &pos)) {
      return false;
    } else {
      uint32_t idx = size_;
      while (idx > pos) {
        array_[idx] = std::move(array_[idx - 1]);
        idx--;
      }
      array_[pos] = std::make_pair(key, value);
      size_++;
      return true;
    }

  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  if (size_ != 0) {
    if (cmp(key, array_[0].first) == 0) {
      size_--;
      uint32_t idx = 0;
      while (idx < size_) {
        array_[idx] = std::move(array_[idx + 1]);
        idx++;
      }
      return true;
    }

    if (cmp(key, array_[size_ - 1].first) == 0) {
      size_--;
      return true;
    }    

    uint32_t pos = size_;
    if (bsearch(key, cmp, &pos)) {
      BUSTUB_ASSERT(pos < size_, "remove position is out of bound");
      size_--;
      uint32_t idx = pos;
      while (idx < size_) {
        array_[idx] = std::move(array_[idx + 1]);
        idx++;
      }      
      return true;
    }
    return false;
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  if (bucket_idx < size_) {
    if (bucket_idx < size_ - 1) {
      uint32_t idx = bucket_idx;
      while (idx + 1 < size_) {
        array_[idx] = array_[idx + 1];
        idx++;
      }
    }
    size_--;
  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::bsearch(const K &key, const KC &cmp, uint32_t *pos) const -> bool {
  uint32_t l = 0, r = size_ - 1;
  while (l <= r) {
    uint32_t m = l + (r - l) / 2;
    if (cmp(array_[m].first, key) == 0) {
      *pos = m;
      return true;
    }
    if (cmp(array_[m].first, key) == -1) {
      if (m < size_ - 1) {
        l = m + 1;
      } else {
        break;
      }      
    } else {
      if (m > 0) {
        r = m - 1;
      } else {
        break;
      }
    }
  }
  *pos = l;
  return false;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
