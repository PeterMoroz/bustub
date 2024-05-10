#include "optimizer/optimizer.h"

#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_child = optimized_plan->GetChildAt(0);
    if (limit_child->GetType() == PlanType::Sort) {
      const auto limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto sort_plan = dynamic_cast<const SortPlanNode &>(*limit_child);
      const auto &sort_child = limit_child->GetChildAt(0);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_child, sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
