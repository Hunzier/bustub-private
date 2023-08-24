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
  left_next_ = true;
  has_join_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  Tuple right_tuple;
  RID right_rid;

  while (true) {
    if (left_next_) {
      left_next_ = false;
      has_join_ = false;
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
    }

    bool right_check = right_executor_->Next(&right_tuple, &right_rid);

    if (!right_check) {
      right_executor_->Init();
      left_next_ = true;
    }

    if (plan_->join_type_ == JoinType::INNER) {
      if (!right_check) {
        continue;
      }
    } else {
      if (!right_check) {
        if (!has_join_) {
          has_join_ = true;
          std::vector<Value> values;
          for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
            values.emplace_back(left_tuple_.GetValue(&left_schema, i));
          }
          auto &cols = right_schema.GetColumns();
          for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
            values.emplace_back(ValueFactory::GetNullValueByType(cols[i].GetType()));
          }
          (*tuple) = Tuple(values, &plan_->OutputSchema());
          return true;
        }
        continue;
      }
    }

    auto opt = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
    if (!opt.IsNull() && opt.GetAs<bool>()) {
      has_join_ = true;
      std::vector<Value> values;
      // std::cout << "Start Combine\n";
      for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
        auto attr = left_tuple_.GetValue(&left_schema, i);
        values.emplace_back(attr);
      }
      // std::cout << "Left has add\n";
      for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_schema, i));
      }
      // std::cout << "Right has add\n";
      (*tuple) = Tuple(values, &plan_->OutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
