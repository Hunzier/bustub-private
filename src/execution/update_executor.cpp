//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  first_execution_ = true;
  update_count_ = 0;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    // 删
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index : indexes) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    // 增
    std::vector<Value> value;
    for (auto &a : plan_->target_expressions_) {
      value.emplace_back(a->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }

    (*tuple) = Tuple(value, &child_executor_->GetOutputSchema());

    TupleMeta new_meta{};
    auto opt = table_info_->table_->InsertTuple(new_meta, *tuple);
    if (opt.has_value()) {
      auto inserted_rid = opt.value();
      // Update the indexes
      for (auto &index : indexes) {
        auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, inserted_rid, exec_ctx_->GetTransaction());
      }
    }
    update_count_++;
  }

  if (first_execution_) {
    // Create a tuple with the insert count and return it
    std::vector<Value> values;
    values.emplace_back(INTEGER, update_count_);
    *tuple = Tuple{values, &GetOutputSchema()};
    first_execution_ = false;
    return true;
  }
  return false;  // Return false after returning the insert count tuple
}

}  // namespace bustub
