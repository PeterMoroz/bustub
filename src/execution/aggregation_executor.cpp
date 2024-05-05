//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_executor_(std::move(child_executor))
    , aht_(plan->aggregates_, plan->agg_types_)
    , aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {

    child_executor_->Init();
    aht_.Clear();

    Tuple child_tuple;
    RID child_rid;

    while (child_executor_->Next(&child_tuple, &child_rid)) {
        aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
    }

    if (aht_.Begin() == aht_.End()) {
        std::vector<Value> values;
        const Schema &schema = child_executor_->GetOutputSchema();

        for (const auto& col : schema.GetColumns()) {
            values.push_back({ValueFactory::GetNullValueByType(col.GetType())});
        }

        aht_.InsertCombine(AggregateKey{}, AggregateValue{values});
    }

    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

    if (aht_iterator_ != aht_.End()) {

        if (aht_iterator_.Key().group_bys_.size() != plan_->group_bys_.size()) {
            return false;
        }

        std::vector<Value> values;                
        for (const auto &key : aht_iterator_.Key().group_bys_) {
            values.push_back(key);
        }        
        for (const auto &val : aht_iterator_.Val().aggregates_) {
            values.push_back(val);
        }

        *tuple = Tuple(values, &GetOutputSchema());
        ++aht_iterator_;
        return true;
    }
    
    return false; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
