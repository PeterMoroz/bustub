//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/hash_util.h"
#include "type/value_factory.h"

namespace bustub {

auto GetMatchIndexes(const std::vector<std::vector<Value>> &where, const std::vector<Value> &what)
    -> std::vector<std::size_t> {
  std::vector<std::size_t> indexes;
  std::size_t i = 0;
  while (i < where.size()) {
    std::size_t j = 0;
    BUSTUB_ASSERT(where[i].size() == what.size(), "the dimensions of the values vectors are not equal");
    while (j < what.size()) {
      if (where[i][j].CompareEquals(what[j]) != CmpBool::CmpTrue) {
        break;
      }
      j++;
    }
    if (j == what.size()) {
      indexes.push_back(i);
    }
    i++;
  }
  return indexes;
};

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  std::unordered_map<hash_t, std::vector<Tuple>> ht_right_tuples{};
  std::unordered_map<hash_t, std::vector<std::vector<Value>>> ht_right_values{};

  Tuple tuple;
  RID rid;
  right_child_->Init();
  while (right_child_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    auto key = hash_t{};
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      auto val = expr->Evaluate(&tuple, right_child_->GetOutputSchema());
      values.push_back(val);
      key = HashUtil::CombineHashes(key, HashUtil::HashValue(&val));
    }
    ht_right_tuples[key].push_back(tuple);
    ht_right_values[key].push_back(values);
  }

  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();

  left_child_->Init();
  while (left_child_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    auto key = hash_t{};
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      auto val = expr->Evaluate(&tuple, left_child_->GetOutputSchema());
      values.push_back(val);
      key = HashUtil::CombineHashes(key, HashUtil::HashValue(&val));
    }

    std::vector<std::size_t> tuples_indexes;
    auto it = ht_right_values.find(key);
    if (it != ht_right_values.cend()) {
      tuples_indexes = GetMatchIndexes(it->second, values);
    }

    if (!tuples_indexes.empty()) {
      auto it2 = ht_right_tuples.find(key);
      BUSTUB_ASSERT(it2 != ht_right_tuples.cend(), "the tuple matching the key values not found");
      for (const auto idx : tuples_indexes) {
        values.clear();
        const auto &right_tuple = it2->second[idx];
        for (const auto &col : GetOutputSchema().GetColumns()) {
          auto idx{left_schema.TryGetColIdx(col.GetName())};
          if (idx) {
            values.push_back(tuple.GetValue(&left_schema, idx.value()));
          } else {
            idx = right_schema.TryGetColIdx(col.GetName());
            if (idx) {
              values.push_back(right_tuple.GetValue(&right_schema, idx.value()));
            }
          }
        }
        output_.push_back(Tuple{values, &GetOutputSchema()});
      }

    } else if (plan_->join_type_ == JoinType::LEFT) {
      values.clear();
      for (const auto &col : GetOutputSchema().GetColumns()) {
        auto idx{left_schema.TryGetColIdx(col.GetName())};
        if (idx) {
          values.push_back(tuple.GetValue(&left_schema, idx.value()));
        } else {
          values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
      }
      output_.push_back(Tuple{values, &GetOutputSchema()});
    }
  }

  out_idx_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_idx_ < output_.size()) {
    *tuple = output_[out_idx_++];
    return true;
  }
  return false;
}

}  // namespace bustub
