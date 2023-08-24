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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  first_check_ = true;
  child_executor_->Init();
  aht_.Clear();
  
  Tuple ntuple;
  RID nrid;
  while (child_executor_->Next(&ntuple, &nrid)) {
    auto agg_key = MakeAggregateKey(&ntuple);
    auto agg_val = MakeAggregateValue(&ntuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (first_check_ && plan_->group_bys_.empty()) {
      first_check_ = false;
      std::vector<Value> values;
      for (const auto &agg_type : plan_->agg_types_) {
        switch (agg_type) {
          case AggregationType::CountStarAggregate:
            // Count start starts at zero.
            values.emplace_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            // Others starts at null.
            values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }

    return false;
  }

  first_check_ = false;
  // Get the next aggregate result from the hash table iterator
  const auto &agg_key = aht_iterator_.Key();
  const auto &agg_val = aht_iterator_.Val();
  // Generate output tuple using the aggregate result
  std::vector<Value> values;
  for (auto &v : agg_key.group_bys_) {
    values.emplace_back(v);
  }

  for (auto &v : agg_val.aggregates_) {
    values.emplace_back(v);
  }

  *tuple = Tuple(values, &GetOutputSchema());

  // Move to the next aggregate result
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
