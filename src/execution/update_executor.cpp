//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void UpdateUndoTuple(std::vector<bool> &modified1, Tuple &tuple1, 
                    const std::vector<bool> &modified2, const Tuple &tuple2,
                    const Schema *schema) {
  BUSTUB_ASSERT(modified1.size() == modified2.size(), "incompatible vectors' dimensions");
  if (modified1 != modified2) {
    const std::size_t n = modified1.size();
    std::vector<Column> columns1, columns2, columns;
    std::vector<bool> modified(n, false);
    std::vector<Value> values;

    for (std::size_t i = 0; i < n; i++) {
      if (modified1[i]) {
        columns1.push_back(schema->GetColumn(i));
        modified[i] = true;
      }
      if (modified2[i]) {
        modified[i] = true;
      }      
    }

    for (std::size_t i = 0; i < n; i++) {
      if (modified[i]) {
        const auto column = schema->GetColumn(i);
        columns.push_back(column);
        values.push_back(Value(column.GetType()));
      }
    }

    const Schema res_schema(columns);
    const Schema schema1(columns1);
    std::size_t idx = 0, idx1 = 0;
    for (std::size_t i = 0; i < n; i++) {
      if (modified1[i]) {
        values[idx++] = tuple1.GetValue(&schema1, idx1++);
      } else if (modified2[i]) {
        values[idx++] = tuple2.GetValue(schema, i);
      }
    }
    
    modified1 = modified;
    tuple1 = Tuple(values, &res_schema);
  }
}

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();

  // Tuple child_tuple;
  // RID child_rid;
  // int32_t num_updated_count = 0;
  // while (child_executor_->Next(&child_tuple, &child_rid)) {
  //   num_updated_count++;

  //   std::vector<Value> values;
  //   for (const auto &ex : plan_->target_expressions_) {
  //     values.push_back(ex->Evaluate(&child_tuple, table_info_->schema_));
  //   }

  //   Tuple tuple_to_insert(values, &table_info_->schema_);
  //   auto t = table_info_->table_->GetTuple(child_rid);

  //   if (!t.first.is_deleted_) {
  //     table_info_->table_->UpdateTupleMeta(TupleMeta{t.first.ts_, true}, child_rid);
  //     for (auto index_info : indexes_) {
  //       const Tuple key{t.second.KeyFromTuple(child_executor_->GetOutputSchema(), *index_info->index_->GetKeySchema(),
  //                                             index_info->index_->GetKeyAttrs())};
  //       index_info->index_->DeleteEntry(key, child_rid, nullptr);
  //     }

  //     auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{t.first.ts_, false}, tuple_to_insert);
  //     BUSTUB_ASSERT(inserted_rid, "insertion failed");
  //     for (auto index_info : indexes_) {
  //       const Tuple key{tuple_to_insert.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
  //                                                    index_info->index_->GetKeyAttrs())};
  //       BUSTUB_ASSERT(index_info->index_->InsertEntry(key, inserted_rid.value(), nullptr), "insert index entry failed");
  //     }
  //   }
  // }


  auto tx = exec_ctx_->GetTransaction();
  auto tx_manager = exec_ctx_->GetTransactionManager();  

  Tuple child_tuple;
  RID child_rid;
  int32_t num_updated_count = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto [tmeta, tuple] = table_info_->table_->GetTuple(child_rid);    
    bool self_modified = tx->GetTransactionTempTs() == tmeta.ts_;
    if (!self_modified) {
      if (tmeta.ts_ > TXN_START_ID || tmeta.ts_ > tx->GetReadTs()) {
        tx->SetTainted();
        throw ExecutionException("write-write conflict when update");
      }
    }

    if (!tmeta.is_deleted_) {

      std::vector<Value> update_values;
      for (const auto &ex : plan_->target_expressions_) {
        update_values.push_back(ex->Evaluate(&child_tuple, table_info_->schema_));
      }

      Tuple update_tuple(update_values, &table_info_->schema_);       

      std::vector<Value> delta_values;
      std::vector<Column> delta_columns;
      const auto schema = table_info_->schema_;
      std::vector<bool> is_modified(schema.GetColumnCount(), false);
      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        const auto value = child_tuple.GetValue(&schema, i);
        if (!value.CompareExactlyEquals(update_tuple.GetValue(&schema, i))) {
          delta_columns.emplace_back(schema.GetColumn(i));
          delta_values.emplace_back(value);
          is_modified[i] = true;
        }
      }

      if (!delta_values.empty()) {
        num_updated_count++;

        const auto undo_link = tx_manager->GetUndoLink(child_rid);
        if (self_modified) {
          if (undo_link.has_value()) {
            BUSTUB_ASSERT((*undo_link).prev_txn_ == tx->GetTransactionId(), "the transaction ID in undo_link doesn't match ID of the current transaction");
            auto undo_log = tx->GetUndoLog((*undo_link).prev_log_idx_);
            UpdateUndoTuple(undo_log.modified_fields_, undo_log.tuple_, is_modified, child_tuple, &schema);
            tx->ModifyUndoLog((*undo_link).prev_log_idx_, 
                UndoLog{false, undo_log.modified_fields_, undo_log.tuple_, undo_log.ts_, undo_log.prev_version_});
          }
        } else {
          const Schema delta_tuple_schema(delta_columns);
          const Tuple delta_tuple(delta_values, &delta_tuple_schema);
          UndoLog undo_log{ false, is_modified, delta_tuple, tmeta.ts_, undo_link.has_value() ? *undo_link : UndoLink{} };
          tx_manager->UpdateUndoLink(child_rid, tx->AppendUndoLog(undo_log));
        }

        table_info_->table_->UpdateTupleInPlace(TupleMeta{tx->GetTransactionTempTs(), false}, update_tuple, child_rid);
        tx->AppendWriteSet(plan_->GetTableOid(), child_rid);
      }
    }
  }

  const auto &schema = GetOutputSchema();
  output_ = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_updated_count)}, &schema);
  consumed_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!consumed_) {
    *tuple = output_;
    consumed_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
