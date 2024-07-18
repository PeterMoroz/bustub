//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_->Init();

  Tuple child_tuple;
  RID child_rid;
  int32_t num_inserted_count = 0;
  auto tx = exec_ctx_->GetTransaction();
  const auto tx_temp_ts = tx->GetTransactionTempTs();

  while (child_->Next(&child_tuple, &child_rid)) {
    std::vector<RID> to_update;
    for (auto index_info : indexes_) {
      const Tuple key{child_tuple.KeyFromTuple(child_->GetOutputSchema(), *index_info->index_->GetKeySchema(),
                                               index_info->index_->GetKeyAttrs())};
      std::vector<RID> rids;
      index_info->index_->ScanKey(key, &rids, tx);

      for (const auto rid : rids) {
        const auto [curr_meta, curr_tuple] = table_info_->table_->GetTuple(rid);
        if (!curr_meta.is_deleted_) {
          tx->SetTainted();
          throw ExecutionException("the tuple already exists");
        }
        to_update.push_back(rid);
      }
    }

    if (to_update.empty()) {
      auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{tx_temp_ts, false}, child_tuple);
      BUSTUB_ASSERT(inserted_rid, "insertion failed");
      exec_ctx_->GetTransactionManager()->UpdateUndoLink(*inserted_rid, std::nullopt);
      tx->AppendWriteSet(plan_->GetTableOid(), *inserted_rid);

      for (auto index_info : indexes_) {
        const Tuple key{child_tuple.KeyFromTuple(child_->GetOutputSchema(), *index_info->index_->GetKeySchema(),
                                                 index_info->index_->GetKeyAttrs())};
        if (!index_info->index_->InsertEntry(key, inserted_rid.value(), tx)) {
          tx->SetTainted();
          throw ExecutionException("unique key constraint violation");
        }
      }
      num_inserted_count++;
    } else {
      auto tx_manager = exec_ctx_->GetTransactionManager();
      const auto schema = table_info_->schema_;
      for (const auto rid : to_update) {
        auto [tmeta, tuple, undo_link] = GetTupleAndUndoLink(tx_manager, table_info_->table_.get(), rid);
        if (tmeta.ts_ == tx->GetTransactionTempTs()) {
          table_info_->table_->UpdateTupleInPlace(TupleMeta{tx->GetTransactionTempTs(), false}, child_tuple, rid,
                                                  nullptr);
        } else {
          UndoLog undo_log{true, {}, tuple, tmeta.ts_, undo_link.has_value() ? *undo_link : UndoLink{}};
          if (!UpdateTupleAndUndoLink(tx_manager, rid, tx->AppendUndoLog(undo_log), table_info_->table_.get(), tx,
                                      TupleMeta{tx->GetTransactionTempTs(), false}, child_tuple)) {
            std::cout << "UpdateTupleAndUndoLink failed ! " << std::endl;
          }
        }
      }
    }
  }

  const auto &schema = GetOutputSchema();
  output_ = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_inserted_count)}, &schema);
  consumed_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!consumed_) {
    *tuple = output_;
    consumed_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
