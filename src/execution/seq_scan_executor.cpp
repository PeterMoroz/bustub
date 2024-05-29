//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto tx = exec_ctx_->GetTransaction();
  if (table_iterator_) {
    while (!table_iterator_->IsEnd()) {
      auto [curr_meta, curr_tuple] = table_iterator_->GetTuple();
      const auto curr_rid = table_iterator_->GetRID();
      ++(*table_iterator_);
      if (!curr_meta.is_deleted_) {
        if (plan_->filter_predicate_) {
          auto v = plan_->filter_predicate_->Evaluate(&curr_tuple, table_info_->schema_);
          if (v.GetAs<int8_t>() == 0) {
            continue;
          }
        }

        if (curr_meta.ts_ == tx->GetReadTs() || curr_meta.ts_ == tx->GetTransactionTempTs()) {
          *tuple = curr_tuple;
          *rid = curr_rid;
          return true;
        } else {
          const auto tx_manager = exec_ctx_->GetTransactionManager();
          const auto undo_link_1st = tx_manager->GetUndoLink(curr_rid);
          if (undo_link_1st.has_value()) {
            auto undo_link = *undo_link_1st;
            std::vector<UndoLog> undo_logs;
            while (true) {
              const auto undo_log = tx_manager->GetUndoLogOptional(undo_link);
              if (undo_log.has_value()) {
                if ((*undo_log).ts_ <= tx->GetReadTs()) {
                  undo_logs.push_back(*undo_log);
                }
                undo_link = undo_log->prev_version_;
              } else {
                break;
              }
            }
            if (!undo_logs.empty()) {
              auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), curr_tuple, curr_meta, undo_logs);
              if (reconstructed_tuple.has_value()) {
                *tuple = *reconstructed_tuple;
                *rid = curr_rid;
                return true;
              }
            }
          }

          continue;
        }
      } else {

        const auto tx_manager = exec_ctx_->GetTransactionManager();
        const auto undo_link_1st = tx_manager->GetUndoLink(curr_rid);
        if (undo_link_1st.has_value()) {
          auto undo_link = *undo_link_1st;
          std::vector<UndoLog> undo_logs;
          while (true) {
            const auto undo_log = tx_manager->GetUndoLogOptional(undo_link);
            if (undo_log.has_value()) {
              if ((*undo_log).ts_ <= tx->GetReadTs()) {
                undo_logs.push_back(*undo_log);
              }
              undo_link = undo_log->prev_version_;
            } else {
              break;
            }
          }
          if (!undo_logs.empty()) {
            auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), curr_tuple, curr_meta, undo_logs);
            if (reconstructed_tuple.has_value()) {
              *tuple = *reconstructed_tuple;
              *rid = curr_rid;
              return true;
            }
          }
        }
        continue;
      }
    }
  }
  return false;
}

}  // namespace bustub
