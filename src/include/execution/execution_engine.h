//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// Identification: src/include/execution/execution_engine.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/executors/init_check_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The ExecutionEngine class executes query plans.
 */
class ExecutionEngine {
 public:
  /**
   * Construct a new ExecutionEngine instance.
   * @param bpm The buffer pool manager used by the execution engine
   * @param txn_mgr The transaction manager used by the execution engine
   * @param catalog The catalog used by the execution engine
   */
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_{bpm}, txn_mgr_{txn_mgr}, catalog_{catalog} {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  /*
  这段代码实现了执行查询计划的功能。执行过程如下：

  首先，通过传入的查询计划 plan 和事务上下文 txn，使用 ExecutorFactory::CreateExecutor 方法构造一个与抽象计划节点相对应的执行器（executor）。
  然后，对执行器进行初始化，调用 executor->Init() 方法进行初始化操作。
  接下来，通过调用 PollExecutor 方法，执行查询计划并逐步获取结果。这个方法会不断从执行器中获取下一个结果元组，并将其添加到结果集 result_set 中，直到没有更多结果为止。
  在执行的过程中，可能会抛出 ExecutionException 异常。如果出现异常，将 executor_succeeded 标志设置为 false，并清空结果集（如果 result_set 非空）。
  最后，执行一些检查操作，调用 PerformChecks 方法来执行一些额外的检查。
  该函数返回一个布尔值，表示查询计划的执行是否成功。如果执行成功，则返回 true，否则返回 false。

  总体而言，该函数通过调用相应的执行器执行查询计划，并将执行过程中产生的结果逐步添加到结果集中。同时，还处理了可能出现的异常情况，并提供了执行过程的错误处理和检查机制。
  */

  /**
   * Execute a query plan.
   * @param plan The query plan to execute
   * @param result_set The set of tuples produced by executing the plan
   * @param txn The transaction context in which the query executes
   * @param exec_ctx The executor context in which the query executes
   * @return `true` if execution of the query plan succeeds, `false` otherwise
   */
  // NOLINTNEXTLINE
  auto Execute(const AbstractPlanNodeRef &plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) -> bool {
    BUSTUB_ASSERT((txn == exec_ctx->GetTransaction()), "Broken Invariant");

    // Construct the executor for the abstract plan node
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // Initialize the executor
    auto executor_succeeded = true;

    try {
      executor->Init();
      PollExecutor(executor.get(), plan, result_set);
      PerformChecks(exec_ctx);
    } catch (const ExecutionException &ex) {
      executor_succeeded = false;
      if (result_set != nullptr) {
        result_set->clear();
      }
    }

    return executor_succeeded;
  }

  void PerformChecks(ExecutorContext *exec_ctx) {
    for (const auto &[left_executor, right_executor] : exec_ctx->GetNLJCheckExecutorSet()) {
      auto casted_left_executor = dynamic_cast<const InitCheckExecutor *>(left_executor);
      auto casted_right_executor = dynamic_cast<const InitCheckExecutor *>(right_executor);
      BUSTUB_ASSERT(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount(),
                    "nlj check failed, are you initialising the right executor every time when there is a left tuple? "
                    "(off-by-one is okay)");
    }
  }

 private:
  /**
   * Poll the executor until exhausted, or exception escapes.
   * @param executor The root executor
   * @param plan The plan to execute
   * @param result_set The tuple result set
   */
  static void PollExecutor(AbstractExecutor *executor, const AbstractPlanNodeRef &plan,
                           std::vector<Tuple> *result_set) {
    RID rid{};
    Tuple tuple{};
    while (executor->Next(&tuple, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(tuple);
      }
    }
  }

  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] TransactionManager *txn_mgr_;
  [[maybe_unused]] Catalog *catalog_;
};

}  // namespace bustub
