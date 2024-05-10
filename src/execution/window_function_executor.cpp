#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

auto SortTuples(std::vector<Tuple> &tuples, const Schema &schema,
                const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys) -> void {
    std::sort(tuples.begin(), tuples.end(), [&](const Tuple &lhs, const Tuple &rhs) {
        for (const auto [type, expr] : order_bys) {
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
}

auto CalculateColumn(const AbstractExpressionRef &function, WindowFunctionType type, 
                const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
                TypeId column_type, bool repeat_value, const Schema &schema,
                std::map<uint32_t, std::vector<Tuple>> &partitions,
                std::vector<Value> &column_values) -> void {
    column_values.clear();
    Value value;

    for (const auto &part : partitions) {

        switch (type) {
            case WindowFunctionType::CountStarAggregate:
                value = ValueFactory::GetZeroValueByType(column_type);
            break;
            case WindowFunctionType::CountAggregate:
                value = ValueFactory::GetZeroValueByType(column_type);
            break;
            case WindowFunctionType::SumAggregate:
                value = ValueFactory::GetZeroValueByType(column_type);
            break;
            case WindowFunctionType::MinAggregate:
                value = ValueFactory::GetNullValueByType(column_type);
            break;
            case WindowFunctionType::MaxAggregate:
                value = ValueFactory::GetNullValueByType(column_type);
            break;
            case WindowFunctionType::Rank:
                value = ValueFactory::GetNullValueByType(column_type);
            break;
        }

        for (const auto &tuple : part.second) {
            const auto tmp = function->Evaluate(&tuple, schema);
            if (!tmp.IsNull()) {
                switch (type) {
                    case WindowFunctionType::CountStarAggregate:
                        value = value.Add(ValueFactory::GetIntegerValue(1));
                    break;
                    case WindowFunctionType::CountAggregate:
                        value = value.Add(ValueFactory::GetIntegerValue(1));
                    break;
                    case WindowFunctionType::SumAggregate:
                        value = value.Add(tmp);
                    break;
                    case WindowFunctionType::MinAggregate:
                        if (value.IsNull() || tmp.CompareLessThan(value) == CmpBool::CmpTrue) {
                            value = tmp;
                        }
                    break;
                    case WindowFunctionType::MaxAggregate:
                        if (value.IsNull() || tmp.CompareGreaterThan(value) == CmpBool::CmpTrue) {
                            value = tmp;
                        }
                    break;
                    case WindowFunctionType::Rank:
                        BUSTUB_ASSERT(!order_by.empty(), "rank function can not be used without order by");
                        const auto order_by_item = order_by.front();
                        auto it = std::find_if(part.second.cbegin(), part.second.cend(), 
                            [&](const Tuple &t)
                            {
                                const auto &lhs = order_by_item.second->Evaluate(&tuple, schema);
                                const auto &rhs = order_by_item.second->Evaluate(&t, schema);
                                return lhs.CompareEquals(rhs) == CmpBool::CmpTrue;
                            });
                        if (it != part.second.cend()) {
                            BUSTUB_ASSERT(order_by_item.first != OrderByType::INVALID, "invalid order by direction");
                            auto pos = std::distance(part.second.cbegin(), it);
                            value = ValueFactory::GetIntegerValue(pos + 1);
                        }
                    break;
                }
            }
            if (!repeat_value) {
                column_values.push_back(value);
            }
        }

        if (repeat_value) {
            const std::size_t n = part.second.size();
            std::fill_n(std::back_inserter(column_values), n, value);
        }
    }
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() { 
    child_executor_->Init();

    Tuple tuple;
    RID rid;

    const auto &window_functions = const_cast<WindowFunctionPlanNode *>(plan_)->window_functions_;
    BUSTUB_ASSERT(!window_functions.empty(), "no window functions provided");

    std::unordered_map<uint32_t, std::vector<Tuple>> tuples;
    while (child_executor_->Next(&tuple, &rid)) {
        for (std::size_t i = 0; i < plan_->columns_.size(); i++) {
            tuples[i].push_back(tuple);
        }
    }

    for (const auto &item : plan_->window_functions_) {
        const auto &order_by = item.second.order_by_;
        if (!order_by.empty()) {
            for (auto &item : tuples) {
                SortTuples(item.second, GetOutputSchema(), order_by);
            }
        }
    }

    std::vector<std::map<uint32_t, std::vector<Tuple>>> partitions;
    for (uint32_t i = 0; i < plan_->columns_.size(); i++) {
        std::map<uint32_t, std::vector<Tuple>> part;
        const auto it = plan_->window_functions_.find(i);
        if (it != plan_->window_functions_.cend() && !it->second.partition_by_.empty()) {
            for (auto &tuple : tuples[i]) {
                const auto pkey = it->second.partition_by_.front()->Evaluate(&tuple, child_executor_->GetOutputSchema());
                part[pkey.GetAs<uint32_t>()].push_back(tuple);
            }            
        } else {
            part[0] = std::move(tuples[i]);
        }
        partitions.push_back(part);
    }

    std::vector<std::vector<Value>> grid;
    for (uint32_t idx = 0; idx < plan_->columns_.size(); idx++) {
        std::vector<Value> column_values;
        const auto &column = plan_->columns_[idx];
        const auto column_value_expression = dynamic_cast<const ColumnValueExpression *>(column.get());
        BUSTUB_ASSERT(column_value_expression, "the column expression expected");        
        if (column_value_expression->GetColIdx() < child_executor_->GetOutputSchema().GetColumns().size()) {
            for (const auto &tuple : partitions[idx][0]) {
                column_values.push_back(tuple.GetValue(&child_executor_->GetOutputSchema(), column_value_expression->GetColIdx()));
            }
        } else {
            auto &wf = plan_->window_functions_.at(idx);
            const auto column_type = column_value_expression->GetReturnType().GetType();
            const bool repeat_value = wf.order_by_.empty(); // && wf.partition_by_.empty();
            CalculateColumn(wf.function_, wf.type_, wf.order_by_, column_type, repeat_value,
                            child_executor_->GetOutputSchema(), partitions[idx], column_values);
        }
        grid.push_back(column_values);
    }

    if (!grid.empty()) {
        for (std::size_t i = 0; i < grid[0].size(); i++) {
            std::vector<Value> tuple_values;
            for (std::size_t j = 0; j < grid.size(); j++) {
                tuple_values.push_back(grid[j][i]);
            }
            output_.push_back(Tuple(tuple_values, &GetOutputSchema()));
        }
    }

    out_idx_ = 0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (out_idx_ < output_.size()) {
        *tuple = output_[out_idx_++];
        return true;
    }
    return false; 
}

}  // namespace bustub
