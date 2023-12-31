//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void DeleteExecutor::Init() {
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
  delete_count_ = 0;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                            LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }

    // Perform the insertion
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index : indexs) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    delete_count_++;
  }

  if (first_execution_) {
    // Create a tuple with the insert count and return it
    std::vector<Value> values;
    values.emplace_back(INTEGER, delete_count_);
    *tuple = Tuple{values, &GetOutputSchema()};
    first_execution_ = false;
    return true;
  }
  return false;  // Return false after returning the insert count tuple
}

}  // namespace bustub
