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
    const auto t = table_info_->table_->GetTuple(*it_);
    if (!t.first.is_deleted_) {
      *tuple = t.second;
      *rid = *it_;
    }
    ++it_;
    return true;
  }
  return false;
}

}  // namespace bustub
