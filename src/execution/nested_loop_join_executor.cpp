//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  Tuple right_tuple;
  Tuple left_tuple;
  RID right_rid;
  RID left_rid;
  std::vector<Value> values;

  const auto schema = plan_->InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  const auto &left_schema{plan_->GetLeftPlan()->OutputSchema()};
  const auto &right_schema{plan_->GetRightPlan()->OutputSchema()};
  bool produce_tuple = false;

  output_.clear();

  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    produce_tuple = false;
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      const auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (value.GetAs<int8_t>()) {
        values.clear();
        for (const auto &col : schema.GetColumns()) {
          auto idx{left_schema.TryGetColIdx(col.GetName())};
          if (idx) {
            values.push_back(left_tuple.GetValue(&left_schema, idx.value()));
          } else {
            idx = right_schema.TryGetColIdx(col.GetName());
            if (idx) {
              values.push_back(right_tuple.GetValue(&right_schema, idx.value()));
            }
          }
        }
        produce_tuple = true;
        output_.push_back(Tuple{values, &schema});
      }
    }

    if (!produce_tuple && plan_->join_type_ == JoinType::LEFT) {
      values.clear();
      for (const auto &col : schema.GetColumns()) {
        auto idx{left_schema.TryGetColIdx(col.GetName())};
        if (idx) {
          values.push_back(left_tuple.GetValue(&left_schema, idx.value()));
        } else {
          values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
      }
      output_.push_back(Tuple{values, &schema});
    }
  }

  out_idx_ = 0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_idx_ < output_.size()) {
    *tuple = output_[out_idx_++];
    return true;
  }

  return false;
}

}  // namespace bustub
