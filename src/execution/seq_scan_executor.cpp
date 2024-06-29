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

      *tuple = curr_tuple;
      *rid = curr_rid;

      auto read_ts = tx->GetReadTs();
      if (curr_meta.ts_ <= read_ts || curr_meta.ts_ == tx->GetTransactionTempTs()) {
        if (curr_meta.is_deleted_) {
          continue;
        }
        if (plan_->filter_predicate_) {
          auto value = plan_->filter_predicate_->Evaluate(&curr_tuple, table_info_->schema_);
          if (!value.IsNull() && value.GetAs<bool>()) {
            *tuple = curr_tuple;
            *rid = curr_rid;
            return true;
          }
          continue;
        }
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
              undo_logs.push_back(*undo_log);
              undo_link = undo_log->prev_version_;
              if (!undo_link.IsValid() || (*undo_log).ts_ <= tx->GetReadTs()) {
                break;
              }
            }
          }
          if (!undo_logs.empty()) {
            if (undo_logs.back().ts_ > tx->GetReadTs()) {
              continue;
            }
            auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), curr_tuple, curr_meta, undo_logs);
            if (reconstructed_tuple.has_value()) {
              if (plan_->filter_predicate_) {
                auto value = plan_->filter_predicate_->Evaluate(&(*reconstructed_tuple), table_info_->schema_);
                if (!value.IsNull() && value.GetAs<bool>()) {
                  *tuple = *reconstructed_tuple;
                  *rid = curr_rid;
                  return true;
                }
                continue;
              }
              *tuple = *reconstructed_tuple;
              *rid = curr_rid;
              return true;
            }
          }
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
