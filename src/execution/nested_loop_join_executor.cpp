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
#include "type/limits.h"
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
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}
void NestedLoopJoinExecutor::Init() {
  // Initialize both child executors
  left_executor_->Init();
  right_executor_->Init();

  left_pos_ = 0;
  right_pos_ = 0;
  left_tuples_.clear();
  right_tuples_.clear();
  matched_ = false;

  RID rid;
  Tuple tuple;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.emplace_back(tuple);
  }

  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }

  for (size_t i = 0; i < left_tuples_.size(); i++) {
    right_executor_->Init();
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (left_tuples_.empty()) {
    return false;
  }

  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();

  if (plan_->GetJoinType() == JoinType::INNER) {
    if (right_tuples_.empty()) {
      return false;
    }

    for (; left_pos_ < left_tuples_.size(); left_pos_++) {
      const auto &left_tuple = left_tuples_[left_pos_];
      for (; right_pos_ < right_tuples_.size();) {
        const auto &right_tuple = right_tuples_[right_pos_++];

        auto match = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        if (!match.IsNull() && match.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
            auto attr = left_tuple.GetValue(&left_schema, i);
            values.emplace_back(attr);
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->output_schema_->GetColumnCount(); i++) {
            auto attr = right_tuple.GetValue(&right_schema, i);
            values.emplace_back(attr);
          }
          (*tuple) = Tuple(values, &GetOutputSchema());
          return true;
        }
      }

      if (right_pos_ >= right_tuples_.size()) {
        right_pos_ = 0;
      }
    }
  } else {
    for (; left_pos_ < left_tuples_.size(); left_pos_++) {
      const auto &left_tuple = left_tuples_[left_pos_];
      for (; right_pos_ < right_tuples_.size();) {
        const auto &right_tuple = right_tuples_[right_pos_++];

        auto match = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        if (!match.IsNull() && match.GetAs<bool>()) {
          matched_ = true;
          std::vector<Value> values;
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
            auto attr = left_tuple.GetValue(&left_schema, i);
            values.emplace_back(attr);
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->output_schema_->GetColumnCount(); i++) {
            auto attr = right_tuple.GetValue(&right_schema, i);
            values.emplace_back(attr);
          }
          (*tuple) = Tuple(values, &GetOutputSchema());
          return true;
        }
      }

      if (right_pos_ >= right_tuples_.size()) {
        right_pos_ = 0;
        if (!matched_) {
          matched_ = true;
          std::vector<Value> values;
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
            auto attr = left_tuple.GetValue(&left_schema, i);
            values.emplace_back(attr);
          }
          auto atts = plan_->GetRightPlan()->output_schema_->GetColumns();
          for (uint32_t i = 0; i < plan_->GetRightPlan()->output_schema_->GetColumnCount(); i++) {
            values.emplace_back(ValueFactory::GetNullValueByType(atts.at(i).GetType()));
          }
          (*tuple) = Tuple(values, &GetOutputSchema());
          return true;
        }
        matched_ = false;
      }
    }
  }
  return false;
}

}  // namespace bustub
