//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/table_heap.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  first_execution_ = true;
  insert_count_ = 0;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Insert tuples into the table and count the number of successful insertions

  RID frid{};
  Tuple ftuple{};
  while (child_executor_->Next(&ftuple, &frid)) {
    // Perform the insertion
    TupleMeta meta{};
    auto opt = table_info_->table_->InsertTuple(meta, ftuple);
    if (opt.has_value()) {
      auto inserted_rid = opt.value();
      // Update the indexes
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index : indexes) {
        auto key = ftuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, inserted_rid, exec_ctx_->GetTransaction());
      }
      insert_count_++;
    }
  }

  if (first_execution_) {
    // Create a tuple with the insert count and return it
    std::vector<Value> values;
    values.emplace_back(INTEGER, insert_count_);
    *tuple = Tuple{values, &GetOutputSchema()};
    first_execution_ = false;
    return true;
  }

  return false;  // Return false after returning the insert count tuple
}

}  // namespace bustub
