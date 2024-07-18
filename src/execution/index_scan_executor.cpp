//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), it_(rids_.cend()) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  for (const auto &pk : plan_->pred_keys_) {
    const ConstantValueExpression *value_expression = dynamic_cast<ConstantValueExpression *>(pk.get());
    const Schema schema{std::vector<Column>{Column("", value_expression->val_.GetTypeId())}};
    const Tuple tuple{std::vector<Value>{value_expression->val_}, &schema};
    std::vector<RID> rids;
    htable_->ScanKey(tuple, &rids, nullptr);
    rids_.insert(rids_.end(), rids.cbegin(), rids.cend());
  }
  it_ = rids_.cbegin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != rids_.cend()) {
    const RID curr_rid = *it_++;
    const auto [curr_meta, curr_tuple] = table_info_->table_->GetTuple(curr_rid);
    const auto tx = exec_ctx_->GetTransaction();
    if (curr_meta.ts_ <= tx->GetReadTs() || curr_meta.ts_ == tx->GetTransactionTempTs()) {
      if (curr_meta.is_deleted_) {
        return false;
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
            return false;
          }
          auto reconstructed_tuple = ReconstructTuple(&GetOutputSchema(), curr_tuple, curr_meta, undo_logs);
          if (reconstructed_tuple.has_value()) {
            *tuple = *reconstructed_tuple;
            *rid = curr_rid;
            return true;
          }
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
