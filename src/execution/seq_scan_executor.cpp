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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_) {
    while (!table_iterator_->IsEnd()) {
      auto t = table_iterator_->GetTuple();
      if (!t.first.is_deleted_) {
        if (plan_->filter_predicate_) {
          auto v = plan_->filter_predicate_->Evaluate(&t.second, table_info_->schema_);
          if (v.GetAs<int8_t>() == 0) {
            ++(*table_iterator_);
            continue;
          }
        }
        *rid = table_iterator_->GetRID();
        *tuple = t.second;
        ++(*table_iterator_);
        return true;
      } else {
        ++(*table_iterator_);
        continue;
      }
    }
  }
  return false;
}

}  // namespace bustub
