#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Check(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_exprs,
           std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
    if (cmp_expr->comp_type_ == ComparisonType::Equal) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
            right_expr != nullptr) {
          // Ensure both exprs have tuple_id == 0
          auto left_expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
          auto right_expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());

          // Now it's in form of <column_expr> = <column_expr>. Let's check if one of them is from the left table, and
          // the other is from the right table.
          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            left_exprs.emplace_back(left_expr_tuple_0);
            right_exprs.emplace_back(right_expr_tuple_0);
            return true;
          }
          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            left_exprs.emplace_back(right_expr_tuple_0);
            right_exprs.emplace_back(left_expr_tuple_0);
            return true;
          }
        }
      }
    }
  } else if (const auto *and_expr = dynamic_cast<const LogicExpression *>(expr.get()); and_expr != nullptr) {
    if (and_expr->logic_type_ == LogicType::And) {
      if (Check(and_expr->children_[0], left_exprs, right_exprs) &&
          Check(and_expr->children_[1], left_exprs, right_exprs)) {
        return true;
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "NestedLoopJoin should have exact two child, That'sweird!");

    const auto predicate = nlj_plan.Predicate();
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    if (Check(predicate, left_exprs, right_exprs)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_exprs, right_exprs,
                                                nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
