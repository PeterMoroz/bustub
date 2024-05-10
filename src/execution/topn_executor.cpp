#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  std::vector<Tuple> tuples;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.push_back(tuple);
  }

  std::sort(tuples.begin(), tuples.end(), [&](const Tuple &lhs, const Tuple &rhs) {
    const auto &schema = child_executor_->GetOutputSchema();
    for (auto [type, expr] : plan_->GetOrderBy()) {
      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        if (expr->Evaluate(&lhs, schema).CompareLessThan(expr->Evaluate(&rhs, schema)) == CmpBool::CmpTrue) {
          return true;
        }
        if (expr->Evaluate(&lhs, schema).CompareGreaterThan(expr->Evaluate(&rhs, schema)) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (expr->Evaluate(&lhs, schema).CompareGreaterThan(expr->Evaluate(&rhs, schema)) == CmpBool::CmpTrue) {
          return true;
        }
        if (expr->Evaluate(&lhs, schema).CompareLessThan(expr->Evaluate(&rhs, schema)) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    return false;
  });

  std::size_t i = 0;
  while (top_entries_.size() < plan_->GetN() && i < tuples.size()) {
    top_entries_.push_back(tuples[i++]);
  }
  out_idx_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_idx_ < top_entries_.size()) {
    *tuple = top_entries_[out_idx_++];
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
