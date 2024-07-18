//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();

  auto tx = exec_ctx_->GetTransaction();
  auto tx_manager = exec_ctx_->GetTransactionManager();

  Tuple child_tuple;
  RID child_rid;
  int32_t num_deleted_count = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto [tmeta, tuple] = table_info_->table_->GetTuple(child_rid);
    const auto tx_temp_ts = tx->GetTransactionTempTs();
    const bool self_modified = tx_temp_ts == tmeta.ts_;
    if (!self_modified) {
      if (tmeta.ts_ > TXN_START_ID || tmeta.ts_ > tx->GetReadTs()) {
        tx->SetTainted();
        throw ExecutionException("write-write conflict when delete");
      }
    }

    num_deleted_count++;

    const auto undo_link = tx_manager->GetUndoLink(child_rid);
    if (!self_modified) {
      UndoLog undo_log{tmeta.is_deleted_, {}, child_tuple, tmeta.ts_, undo_link.has_value() ? *undo_link : UndoLink{}};
      tx_manager->UpdateUndoLink(child_rid, tx->AppendUndoLog(undo_log));
    }

    table_info_->table_->UpdateTupleInPlace(TupleMeta{tx->GetTransactionTempTs(), true}, child_tuple, child_rid);
    tx->AppendWriteSet(plan_->GetTableOid(), child_rid);
  }

  const auto &schema = GetOutputSchema();
  output_ = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_deleted_count)}, &schema);
  consumed_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!consumed_) {
    *tuple = output_;
    consumed_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
