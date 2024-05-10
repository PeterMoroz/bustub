#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { 
    child_executor_->Init();

    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        output_.push_back(tuple);
    }

    std::sort(output_.begin(), output_.end(), [&](const Tuple &lhs, const Tuple &rhs){
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

    out_idx_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (out_idx_ < output_.size()) {
        *tuple = output_[out_idx_++];
        return true;
    }
    return false;
}

}  // namespace bustub
