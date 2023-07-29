//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  std::vector<Value> keys_;
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct JoinValue {
  std::vector<Value> values_;
};
}  // namespace bustub

namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &agg_key) const -> std::size_t {
    size_t cur_hash = 0;
    for (const auto &key : agg_key.keys_) {
      if (!key.IsNull()) {
        cur_hash = bustub::HashUtil::CombineHashes(cur_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return cur_hash;
  }
};
}  // namespace std

namespace bustub {
class JoinHashTable {
 public:
  JoinHashTable() = default;
  void Insert(const JoinKey &join_key, const JoinValue &join_val) {
    auto &values = ht_[join_key];
    values.emplace_back(join_val);
  }

  auto Get(JoinKey &join_key) -> const std::vector<JoinValue> * {
    auto it = ht_.find(join_key);
    if (it == ht_.end()) {
      return nullptr;
    }
    return (&it->second);
  }

  void Clear() { ht_.clear(); }

 private:
  std::unordered_map<JoinKey, std::vector<JoinValue>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an JoinKey */
  auto MakeJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeLeftJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an JoinValue */
  auto MakeJoinValue(const Tuple *tuple) -> JoinValue {
    std::vector<Value> keys;
    for (uint32_t i = 0; i < plan_->GetRightPlan()->output_schema_->GetColumnCount(); i++) {
      keys.emplace_back(tuple->GetValue(&right_child_->GetOutputSchema(), i));
    }
    return {keys};
  }

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  Tuple left_tuple_{};
  RID left_rid_{};
  JoinHashTable jht_;
  const std::vector<JoinValue> *right_tuples_;
  size_t right_pos_;
  bool first_check_;
  bool left_join_check_;
};

}  // namespace bustub
