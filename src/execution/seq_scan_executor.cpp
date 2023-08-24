//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);

      // std::cout << "Start Fuck\n";
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    (*tuple) = table_iter_->GetTuple().second;
    (*rid) = table_iter_->GetRID();

    if (table_info_->table_->GetTupleMeta(*rid).is_deleted_) {
      table_iter_->operator++();
      continue;
    }
    table_iter_->operator++();

    // 锁行
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                              LockManager::LockMode::SHARED, table_info_->oid_, *rid);
        if (!is_locked) {
          throw ExecutionException("SeqScan Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("SeqScan Executor Get Row Lock Failed");
      }
    }
    return true;
  }

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
    table_oid_t oid = table_info_->oid_;
    for (auto rid : locked_row_set) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
    }
    exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
  }
  // std::cout << "False\n";
  return false;
}

}  // namespace bustub
