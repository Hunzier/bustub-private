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
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  jht_.Clear();
  right_pos_ = 0;
  right_tuples_ = nullptr;
  first_check_ = true;
  left_join_check_ = true;

  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto key = MakeJoinKey(&right_tuple);
    auto val = MakeJoinValue(&right_tuple);
    jht_.Insert(key, val);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();

  while (true) {
    if (plan_->GetJoinType() == JoinType::INNER) {
      if (right_tuples_ == nullptr || right_pos_ >= right_tuples_->size()) {
        if (!left_child_->Next(&left_tuple_, &left_rid_)) {
          return false;
        }
        auto key = MakeLeftJoinKey(&left_tuple_);
        right_tuples_ = jht_.Get(key);
        right_pos_ = 0;
      }

      if (right_tuples_ == nullptr) {
        continue;
      }

      auto right_values = right_tuples_->at(right_pos_++);
      std::vector<Value> values;
      for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
        auto attr = left_tuple_.GetValue(&left_schema, i);
        values.emplace_back(attr);
      }
      for (auto &value : right_values.values_) {
        values.emplace_back(value);
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }

    // Left Join
    if (first_check_) {
      first_check_ = false;
      if (!left_child_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }

      auto key = MakeLeftJoinKey(&left_tuple_);
      right_tuples_ = jht_.Get(key);
      right_pos_ = 0;
      left_join_check_ = (right_tuples_ == nullptr);
    }

    if (right_tuples_ == nullptr || right_pos_ >= right_tuples_->size()) {
      if (left_join_check_) {
        left_join_check_ = false;
        std::vector<Value> values;
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
          auto attr = left_tuple_.GetValue(&left_schema, i);
          values.emplace_back(attr);
        }
        auto atts = plan_->GetRightPlan()->output_schema_->GetColumns();
        for (uint32_t i = 0; i < plan_->GetRightPlan()->output_schema_->GetColumnCount(); i++) {
          values.emplace_back(ValueFactory::GetNullValueByType(atts.at(i).GetType()));
        }
        (*tuple) = Tuple(values, &GetOutputSchema());
        return true;
      }

      if (!left_child_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }

      auto key = MakeLeftJoinKey(&left_tuple_);
      right_tuples_ = jht_.Get(key);
      right_pos_ = 0;
      left_join_check_ = (right_tuples_ == nullptr);
    }

    if (right_tuples_ == nullptr) {
      continue;
    }

    auto right_values = right_tuples_->at(right_pos_++);
    std::vector<Value> values;
    for (uint32_t i = 0; i < plan_->GetLeftPlan()->output_schema_->GetColumnCount(); i++) {
      auto attr = left_tuple_.GetValue(&left_schema, i);
      values.emplace_back(attr);
    }

    for (const auto &value : right_values.values_) {
      values.emplace_back(value);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
