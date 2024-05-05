#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

#include <tuple>

namespace bustub {

auto ProcessCompareEqualExpressions(const std::vector<AbstractExpressionRef> &expressions, const TableInfo *table_info) ->
    std::optional<std::pair<std::uint32_t, std::vector<AbstractExpressionRef>>> {
  
  std::vector<AbstractExpressionRef> values_expressions;
  std::uint32_t col_index = std::numeric_limits<std::uint32_t>::max();

  for (const auto expr : expressions) {
    const auto &lchild = expr->GetChildAt(0);
    const auto &rchild = expr->GetChildAt(1);

    const auto *lcolumn = dynamic_cast<const ColumnValueExpression *>(lchild.get());
    const auto *rcolumn = dynamic_cast<const ColumnValueExpression *>(rchild.get());
    const auto *lconstant = dynamic_cast<const ConstantValueExpression *>(lchild.get());
    const auto *rconstant = dynamic_cast<const ConstantValueExpression *>(rchild.get());

    if (lcolumn && rconstant) {
      if (col_index == std::numeric_limits<std::uint32_t>::max()) {
        col_index = lcolumn->GetColIdx();
      }
      if (col_index != lcolumn->GetColIdx()) {
        return std::nullopt;
      }
      values_expressions.emplace_back(rchild);
    } else if (rcolumn && lconstant) {
      if (col_index == std::numeric_limits<std::uint32_t>::max()) {
        col_index = rcolumn->GetColIdx();
      }
      if (col_index != rcolumn->GetColIdx()) {
        return std::nullopt;
      }
      values_expressions.emplace_back(lchild);
    }
  }

  for (size_t i = 0; i < values_expressions.size() - 1; i++) {
    const auto *curr = dynamic_cast<const ConstantValueExpression *>(values_expressions[i].get());
    for (size_t j = i + 1; j < values_expressions.size(); j++) {
      const auto *next = dynamic_cast<const ConstantValueExpression *>(values_expressions[j].get());
      if (curr->val_.CompareEquals(next->val_) == CmpBool::CmpTrue) {
        values_expressions.erase(values_expressions.begin() + j);
        break;
      }
    }
  }

  if (values_expressions.size() == 2) {
    const auto *curr = dynamic_cast<const ConstantValueExpression *>(values_expressions[0].get());
    const auto *next = dynamic_cast<const ConstantValueExpression *>(values_expressions[1].get());
    if (curr->val_.CompareEquals(next->val_) == CmpBool::CmpTrue) {
      values_expressions.erase(std::next(values_expressions.begin()));
    }
  }

  return std::make_pair(col_index, values_expressions);
}

auto TraverseLogicOr(const AbstractExpressionRef &expr, 
                    std::vector<AbstractExpressionRef> &cmp_eq_expressions) -> void {

  const auto &lchild = expr->GetChildAt(0);
  const auto &rchild = expr->GetChildAt(1);

  {
    const auto *compare_expression = dynamic_cast<ComparisonExpression *>(rchild.get());
    if (compare_expression && compare_expression->comp_type_ == ComparisonType::Equal) {
      cmp_eq_expressions.emplace_back(rchild);
    } else {
      const auto *logic_expression = dynamic_cast<LogicExpression *>(rchild.get());
      if (logic_expression && logic_expression->logic_type_ == LogicType::Or) {
        TraverseLogicOr(rchild, cmp_eq_expressions);
      } else {
        cmp_eq_expressions.clear();
        return ;
      }
    }
  }

  {
    const auto *compare_expression = dynamic_cast<ComparisonExpression *>(lchild.get());
    if (compare_expression && compare_expression->comp_type_ == ComparisonType::Equal) {
      cmp_eq_expressions.emplace_back(lchild);
    } else {
      const auto *logic_expression = dynamic_cast<LogicExpression *>(lchild.get());
      if (logic_expression && logic_expression->logic_type_ == LogicType::Or) {
        TraverseLogicOr(lchild, cmp_eq_expressions);
      } else {
        cmp_eq_expressions.clear();
        return ;
      }
    }
  }

}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    if (seq_scan.filter_predicate_) {      
      if (const auto *expr = dynamic_cast<const LogicExpression *>(seq_scan.filter_predicate_.get()); expr != nullptr) {
        if (expr->logic_type_ == LogicType::Or) {
          std::vector<AbstractExpressionRef> cmp_eq_children;
          TraverseLogicOr(seq_scan.filter_predicate_, cmp_eq_children);
          if (!cmp_eq_children.empty()) {
            auto predicates = ProcessCompareEqualExpressions(cmp_eq_children, catalog_.GetTable(seq_scan.GetTableOid()));
            if (predicates.has_value()) {
              const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
              auto index_info = MatchIndex(table_info->name_, std::get<0>(predicates.value()));
              if (index_info.has_value()) {
                return std::make_shared<IndexScanPlanNode>(plan->output_schema_, seq_scan.GetTableOid(), 
                                                          std::get<0>(index_info.value()), nullptr,
                                                          std::get<1>(predicates.value()));
              }
            }
          }
        }
      } else {
        const auto *comparison_expression = dynamic_cast<ComparisonExpression *>(seq_scan.filter_predicate_.get());
        if (comparison_expression && comparison_expression->comp_type_ == ComparisonType::Equal) {
          const auto &lchild = comparison_expression->GetChildAt(0);
          const auto &rchild = comparison_expression->GetChildAt(1);

          const auto *lcolumn = dynamic_cast<ColumnValueExpression *>(lchild.get());
          const auto *rcolumn = dynamic_cast<ColumnValueExpression *>(rchild.get());
          const auto *lconstant = dynamic_cast<ConstantValueExpression *>(lchild.get());
          const auto *rconstant = dynamic_cast<ConstantValueExpression *>(rchild.get());

          if (lcolumn && rconstant) {
            const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
            auto index_info = MatchIndex(table_info->name_, lcolumn->GetColIdx());
            if (index_info.has_value()) {
              return std::make_shared<IndexScanPlanNode>(plan->output_schema_,
                                                      seq_scan.GetTableOid(), 
                                                      std::get<0>(index_info.value()), nullptr,
                                                      std::vector<AbstractExpressionRef>{rchild});
            }
          } else if (rcolumn && lconstant) {
            const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
            auto index_info = MatchIndex(table_info->name_, rcolumn->GetColIdx());            
            if (index_info.has_value()) {
              return std::make_shared<IndexScanPlanNode>(plan->output_schema_,
                                                      seq_scan.GetTableOid(), 
                                                      std::get<0>(index_info.value()), nullptr,
                                                      std::vector<AbstractExpressionRef>{lchild});
            }
          }

        }
      }
    }
  }
  
  return plan;
}

}  // namespace bustub
