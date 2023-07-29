#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

auto TopNExecutor::Cmp(int u, int v) -> bool {
  --u, --v;
  auto &lhs = heap_[u];
  auto &rhs = heap_[v];
  for (auto &[type, expr] : plan_->GetOrderBy()) {
    auto lv = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
    auto rv = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
    if (lv.CompareEquals(rv) == CmpBool::CmpTrue) {
      continue;
    }
    return static_cast<bool>((lv.CompareLessThan(rv) == CmpBool::CmpTrue) ^ (type != OrderByType::DESC));
  }
  return false;
}

void TopNExecutor::Swap(int u, int v) {
  --u;
  --v;
  std::swap(heap_[u], heap_[v]);
}

void TopNExecutor::Down(int u) {
  int t = u;
  if (u * 2 <= static_cast<int>(heap_.size()) && Cmp(u * 2, t)) {
    t = u * 2;
  }
  if (u * 2 + 1 <= static_cast<int>(heap_.size()) && Cmp(u * 2 + 1, t)) {
    t = u * 2 + 1;
  }

  if (t != u) {
    Swap(t, u);
    Down(t);
  }
}

void TopNExecutor::Up(int u) {
  if (((u / 2) != 0) && Cmp(u, u / 2)) {
    Swap(u, u / 2);
    Up(u / 2);
  }
}

void TopNExecutor::Init() {
  child_executor_->Init();
  heap_.clear();

  RID rid;
  Tuple tuple;
  while (child_executor_->Next(&tuple, &rid)) {
    heap_.emplace_back(tuple);
    Up(heap_.size());
    if (heap_.size() > plan_->n_) {
      Swap(1, heap_.size());
      heap_.pop_back();
      Down(1);
    }
  }

  // for (auto & i : heap_) {
  //     std::cout << i.GetValue(&child_executor_->GetOutputSchema(), 0).GetAs<int>() << "\n";
  // }

  while (!heap_.empty()) {
    tuples_.emplace_back(heap_[0]);
    Swap(1, heap_.size());
    heap_.pop_back();
    if (heap_.size() > 1) {
      Down(1);
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  std::vector<Value> values;
  const auto &cur_tuple = tuples_.back();
  values.reserve(plan_->output_schema_->GetColumnCount());
  for (uint32_t i = 0; i < plan_->output_schema_->GetColumnCount(); i++) {
    values.emplace_back(cur_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
  }
  (*tuple) = Tuple(values, &GetOutputSchema());
  tuples_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
