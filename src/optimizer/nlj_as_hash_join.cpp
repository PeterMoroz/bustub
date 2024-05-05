#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"

#include "execution/expressions/logic_expression.h"

#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto TraverseLogicalAnd(const AbstractExpressionRef &expr, 
                      std::vector<AbstractExpressionRef> &cmp_expressions) -> void {
  const auto &lchild = expr->GetChildAt(0);
  const auto &rchild = expr->GetChildAt(1);

  {
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(lchild.get()); expr != nullptr) {
      cmp_expressions.emplace_back(lchild);
    } else {
      if (const auto *expr = dynamic_cast<const LogicExpression *>(lchild.get()); expr != nullptr) {
        if (expr->logic_type_ == LogicType::And) {
          TraverseLogicalAnd(lchild, cmp_expressions);
        } else {
          cmp_expressions.clear();
          return;
        }
      }
    }
  }

  {
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(rchild.get()); expr != nullptr) {
      cmp_expressions.emplace_back(rchild);
    } else {
      if (const auto *expr = dynamic_cast<const LogicExpression *>(rchild.get()); expr != nullptr) {
        if (expr->logic_type_ == LogicType::And) {
          TraverseLogicalAnd(rchild, cmp_expressions);
        } else {
          cmp_expressions.clear();
          return;
        }
      }
    }
  }  
}

auto GetKeyExpressions(const std::vector<AbstractExpressionRef> &cmp_eq_expressions,
                        std::vector<AbstractExpressionRef> &left_key_expressions,
                        std::vector<AbstractExpressionRef> &right_key_expressions) -> void
{
  for (const auto &expr : cmp_eq_expressions) {
    const auto &lchild = expr->GetChildAt(0);
    const auto &rchild = expr->GetChildAt(1);

    if (const auto *lexpr = dynamic_cast<const ColumnValueExpression *>(lchild.get()); lexpr != nullptr) {
      if (lexpr->GetTupleIdx() == 0) {
        left_key_expressions.push_back(lchild);
      } else if (lexpr->GetTupleIdx() == 1) {
        right_key_expressions.push_back(lchild);
      }
    }

    if (const auto *rexpr = dynamic_cast<const ColumnValueExpression *>(rchild.get()); rexpr != nullptr) {
      if (rexpr->GetTupleIdx() == 0) {
        left_key_expressions.push_back(rchild);
      } else if (rexpr->GetTupleIdx() == 1) {
        right_key_expressions.push_back(rchild);
      }
    }
  }

}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));        

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have only 2 children");
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        std::vector<AbstractExpressionRef> cmp_eq_children;
        TraverseLogicalAnd(nlj_plan.Predicate(), cmp_eq_children);
        if (!cmp_eq_children.empty()) {
          std::vector<AbstractExpressionRef> left_expressions;
          std::vector<AbstractExpressionRef> right_expressions;
          GetKeyExpressions(cmp_eq_children, left_expressions, right_expressions);

          if (!left_expressions.empty() && !right_expressions.empty()) {
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, 
                nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                left_expressions, right_expressions, nlj_plan.GetJoinType());
          }
        }
      }
    } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        const auto &lchild = expr->GetChildAt(0);
        const auto &rchild = expr->GetChildAt(1);

        const auto *lcolumn_expr = dynamic_cast<const ColumnValueExpression *>(lchild.get());
        const auto *rcolumn_expr = dynamic_cast<const ColumnValueExpression *>(rchild.get());

        if (lcolumn_expr != nullptr && rcolumn_expr != nullptr) {
          if (lcolumn_expr->GetTupleIdx() == 0 && rcolumn_expr->GetTupleIdx() == 1) {
            std::vector<AbstractExpressionRef> left_expressions{lchild};
            std::vector<AbstractExpressionRef> right_expressions{rchild};
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, 
                nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                left_expressions, right_expressions, nlj_plan.GetJoinType());            
          } else if (lcolumn_expr->GetTupleIdx() == 1 && rcolumn_expr->GetTupleIdx() == 0) {
            std::vector<AbstractExpressionRef> left_expressions{rchild};
            std::vector<AbstractExpressionRef> right_expressions{lchild};
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, 
                nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                left_expressions, right_expressions, nlj_plan.GetJoinType());
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
