#include "execution/executors/sort_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  index_ = 0;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  sort(tuples_.begin(), tuples_.end(), [&](const Tuple &lhs, const Tuple &rhs) {
    for (auto &[type, expr] : plan_->GetOrderBy()) {
      auto lv = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      auto rv = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lv.CompareEquals(rv) == CmpBool::CmpTrue) {
        continue;
      }
      return static_cast<bool>((lv.CompareLessThan(rv) == CmpBool::CmpTrue) ^ (type == OrderByType::DESC));
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= tuples_.size()) {
    return false;
  }
  std::vector<Value> values;
  const auto &cur_tuple = tuples_[index_++];
  values.reserve(plan_->output_schema_->GetColumnCount());
  for (uint32_t i = 0; i < plan_->output_schema_->GetColumnCount(); i++) {
    values.emplace_back(cur_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
  }
  (*tuple) = Tuple(values, &GetOutputSchema());

  return true;
}

}  // namespace bustub
