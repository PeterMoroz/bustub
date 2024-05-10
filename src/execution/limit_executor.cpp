//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { 
    child_executor_->Init();

    Tuple tuple;
    RID rid;

    while (child_executor_->Next(&tuple, &rid) && output_.size() < plan_->GetLimit()) {
        output_.push_back(tuple);
    }

    out_idx_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (out_idx_ < output_.size()) {
        *tuple = output_[out_idx_++];
        return true;
    }
    return false;
}

}  // namespace bustub
