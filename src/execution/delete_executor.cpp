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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(plan_->table_oid_);
    indexes_ = catalog->GetTableIndexes(table_info_->name_);
    child_executor_->Init();

    Tuple child_tuple;
    RID child_rid;
    int32_t num_deleted_count = 0;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        num_deleted_count++;
        auto t = table_info_->table_->GetTuple(child_rid);
        if (!t.first.is_deleted_) {
            table_info_->table_->UpdateTupleMeta(TupleMeta{t.first.ts_, true}, child_rid);
            for (auto index_info : indexes_) {
                const Tuple key{t.second.KeyFromTuple(child_executor_->GetOutputSchema(), 
                                *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs())};
                index_info->index_->DeleteEntry(key, child_rid, nullptr);
            }
        }
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
