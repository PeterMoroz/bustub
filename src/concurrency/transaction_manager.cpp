//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_.load());

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  const auto wsets = txn->GetWriteSets();
  for (const auto &wset : wsets) {
    auto table_info = catalog_->GetTable(wset.first);
    for (const auto &rid : wset.second) {
      auto tuple_meta = table_info->table_->GetTupleMeta(rid);
      table_info->table_->UpdateTupleMeta(TupleMeta{commit_ts, tuple_meta.is_deleted_}, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->commit_ts_.store(commit_ts);
  last_commit_ts_++;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::unordered_set<txn_id_t> gc_candidates;
  std::unordered_set<txn_id_t> ongoing;

  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);

  for (const auto [txid, tx] : txn_map_) {
    const auto tx_state = tx->GetTransactionState();
    if (tx_state == TransactionState::COMMITTED || tx_state == TransactionState::ABORTED) {
      gc_candidates.insert(txid);
    } else {
      ongoing.insert(txid);
    }
  }

  const auto watermark = running_txns_.GetWatermark();
  for (const auto txid : gc_candidates) {
    const auto tx = txn_map_[txid];
    bool gc_ready = true;
    const std::size_t undo_log_num = tx->GetUndoLogNum();
    for (std::size_t idx = 0; idx < undo_log_num; idx++) {
      const auto undo_log = tx->GetUndoLog(idx);
      if (undo_log.ts_ >= watermark) {
        gc_ready = false;
        break;
      }
    }

    if (gc_ready) {
      const auto tnames{catalog_->GetTableNames()};
      for (const auto tname : tnames) {
        const auto table_info{catalog_->GetTable(tname)};
        const auto &table_heap = table_info->table_;
        auto itr{table_heap->MakeIterator()};
        while (!itr.IsEnd()) {
          const auto [tmeta, tuple] = itr.GetTuple();
          if (tmeta.ts_ == tx->GetCommitTs()) {
            for (const auto txid2 : ongoing) {
              const auto tx2 = txn_map_[txid2];
              if (tx2->GetReadTs() <= tmeta.ts_) {
                gc_ready = false;
                break;
              }
            }
          }

          if (!gc_ready) {
            break;
          }

          ++itr;
        }
        if (gc_ready) {
          break;
        }
      }

      if (gc_ready) {
        running_txns_.RemoveTxn(tx->GetReadTs());
        txn_map_.erase(txid);
      }
    }
  }
}

}  // namespace bustub
