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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  first_execution_ = true;
  insert_count_ = 0;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Insert tuples into the table and count the number of successful insertions

  RID frid{};
  Tuple ftuple{};
  if (first_execution_) {
    while (child_executor_->Next(&ftuple, &frid)) {
      // 上行锁
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                              LockManager::LockMode::EXCLUSIVE, table_info_->oid_, frid);
        if (!is_locked) {
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }

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
