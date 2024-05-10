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

namespace bustub {

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

  Tuple child_tuple;
  RID child_rid;
  int32_t num_updated_count = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    num_updated_count++;

    std::vector<Value> values;
    for (const auto &ex : plan_->target_expressions_) {
      values.push_back(ex->Evaluate(&child_tuple, table_info_->schema_));
    }

    Tuple tuple_to_insert(values, &table_info_->schema_);
    auto t = table_info_->table_->GetTuple(child_rid);

    if (!t.first.is_deleted_) {
      table_info_->table_->UpdateTupleMeta(TupleMeta{t.first.ts_, true}, child_rid);
      for (auto index_info : indexes_) {
        const Tuple key{t.second.KeyFromTuple(child_executor_->GetOutputSchema(), *index_info->index_->GetKeySchema(),
                                              index_info->index_->GetKeyAttrs())};
        index_info->index_->DeleteEntry(key, child_rid, nullptr);
      }

      auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{t.first.ts_, false}, tuple_to_insert);
      BUSTUB_ASSERT(inserted_rid, "insertion failed");
      for (auto index_info : indexes_) {
        const Tuple key{tuple_to_insert.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                     index_info->index_->GetKeyAttrs())};
        BUSTUB_ASSERT(index_info->index_->InsertEntry(key, inserted_rid.value(), nullptr), "insert index entry failed");
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
