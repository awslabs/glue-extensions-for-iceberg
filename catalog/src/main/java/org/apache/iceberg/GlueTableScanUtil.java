/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.iceberg;

import static org.apache.iceberg.expressions.Expression.Operation.AND;
import static org.apache.iceberg.expressions.Expression.Operation.EQ;
import static org.apache.iceberg.expressions.Expression.Operation.GT;
import static org.apache.iceberg.expressions.Expression.Operation.GT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.IN;
import static org.apache.iceberg.expressions.Expression.Operation.LT;
import static org.apache.iceberg.expressions.Expression.Operation.LT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_IN;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_NULL;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_STARTS_WITH;
import static org.apache.iceberg.expressions.Expression.Operation.OR;
import static org.apache.iceberg.expressions.Expression.Operation.STARTS_WITH;

import java.util.Comparator;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Or;

public class GlueTableScanUtil {

  public static boolean canReuseExistingScanExpression(
      Expression existingExpr, Expression newExpr) {
    if (existingExpr == null || newExpr == null) {
      return false;
    }

    // Normalize expressions by rewriting NOT expressions
    Expression rewrittenExistingExpr = Expressions.rewriteNot(existingExpr);
    Expression rewrittenNewExpr = Expressions.rewriteNot(newExpr);
    return rewrittenExistingExpr.isEquivalentTo(Expressions.alwaysTrue())
        || canReuseExpression(rewrittenExistingExpr, rewrittenNewExpr);
  }

  private static boolean canReuseExpression(Expression existingExpr, Expression newExpr) {
    switch (existingExpr.op()) {
      case AND:
        return canReuseAndExpression((And) existingExpr, newExpr);
      case OR:
        return canReuseOrExpression((Or) existingExpr, newExpr);
      default:
        return canReusePredicateExpression(existingExpr, newExpr);
    }
  }

  private static boolean canReuseAndExpression(And existingAndExpr, Expression newExpr) {
    if (newExpr.op() == AND) {
      And newAndExpr = (And) newExpr;
      return canReuseConditionalBranches(
          existingAndExpr.left(), existingAndExpr.right(), newAndExpr.left(), newAndExpr.right());
    }
    boolean leftMatches = canReuseExpression(existingAndExpr.left(), newExpr);
    boolean rightMatches = canReuseExpression(existingAndExpr.right(), newExpr);

    // Equals null safe case: one side matches, and the other is a NOT_NULL check for the same
    // reference
    if ((leftMatches && isNotNullForSameRef(existingAndExpr.right(), newExpr))
        || (rightMatches && isNotNullForSameRef(existingAndExpr.left(), newExpr))) {
      return true;
    }
    return leftMatches && rightMatches;
  }

  private static boolean canReuseOrExpression(Or existingOrExpr, Expression newExpr) {
    if (newExpr.op() == OR) {
      Or newOrExpr = (Or) newExpr;
      return canReuseConditionalBranches(
          existingOrExpr.left(), existingOrExpr.right(), newOrExpr.left(), newOrExpr.right());
    }
    return canReuseExpression(existingOrExpr.left(), newExpr)
        || canReuseExpression(existingOrExpr.right(), newExpr);
  }

  private static boolean canReuseConditionalBranches(
      Expression existingLeftExpr,
      Expression existingRightExpr,
      Expression newLeftExpr,
      Expression newRightExpr) {
    return canReuseExpression(existingLeftExpr, newLeftExpr)
            && canReuseExpression(existingRightExpr, newRightExpr)
        || canReuseExpression(existingLeftExpr, newRightExpr)
            && canReuseExpression(existingRightExpr, newLeftExpr);
  }

  private static boolean canReusePredicateExpression(Expression existingExpr, Expression newExpr) {
    if (existingExpr.isEquivalentTo(newExpr)) {
      return true;
    } else if (newExpr.op() == AND) {
      And newAndExpr = (And) newExpr;
      return canReuseExpression(existingExpr, newAndExpr.right())
          || canReuseExpression(existingExpr, newAndExpr.left());
    } else if (existingExpr instanceof BoundPredicate && newExpr instanceof BoundPredicate) {
      return arePredicateInRange((BoundPredicate<?>) existingExpr, (BoundPredicate<?>) newExpr);
    }
    return false;
  }

  private static boolean arePredicateInRange(
      BoundPredicate<?> existingPred, BoundPredicate<?> newPred) {
    if (!existingPred.ref().isEquivalentTo(newPred.ref())) {
      return false;
    }

    if (existingPred.isLiteralPredicate() && newPred.isLiteralPredicate()) {
      return compareLiteralPredicates(
          existingPred.asLiteralPredicate(), newPred.asLiteralPredicate());
    } else if (existingPred.isSetPredicate() && newPred.isLiteralPredicate()) {
      return compareSetToLiteral(existingPred.asSetPredicate(), newPred.asLiteralPredicate());
    } else if (existingPred.isLiteralPredicate() && newPred.isSetPredicate()) {
      return compareLiteralToSet(existingPred.asLiteralPredicate(), newPred.asSetPredicate());
    } else if (existingPred.isSetPredicate() && newPred.isSetPredicate()) {
      return compareSetToSet(existingPred.asSetPredicate(), newPred.asSetPredicate());
    }
    return false;
  }

  private static boolean compareSetToSet(
      BoundSetPredicate<?> existingPred, BoundSetPredicate<?> newPred) {
    if (existingPred.op() == newPred.op()) {
      if (existingPred.op() == NOT_IN) {
        return newPred.literalSet().containsAll(existingPred.literalSet());
      }
      return existingPred.literalSet().containsAll(newPred.literalSet());
    }
    return false;
  }

  private static boolean compareSetToLiteral(
      BoundSetPredicate<?> existingPred, BoundLiteralPredicate<?> newPred) {
    if (newPred.op() == EQ) {
      if (existingPred.op() == IN) {
        return existingPred.literalSet().contains(newPred.asLiteralPredicate().literal().value());
      } else if (existingPred.op() == NOT_IN) {
        return !existingPred.literalSet().contains(newPred.asLiteralPredicate().literal().value());
      }
    }
    return false;
  }

  private static boolean compareLiteralToSet(
      BoundLiteralPredicate<?> existingLiteralPred, BoundSetPredicate<?> newSetPred) {
    if (newSetPred.op() == IN && existingLiteralPred.op() == EQ) {
      return newSetPred.literalSet().size() == 1
          && newSetPred
              .literalSet()
              .contains(existingLiteralPred.asLiteralPredicate().literal().value());
    } else if (newSetPred.op() == NOT_IN && existingLiteralPred.op() == NOT_EQ) {
      return newSetPred
          .literalSet()
          .contains(existingLiteralPred.asLiteralPredicate().literal().value());
    }
    return false;
  }

  public static boolean compareLiteralPredicates(
      BoundLiteralPredicate<?> existingPred, BoundLiteralPredicate<?> newPred) {
    Literal<?> existingValue = existingPred.literal();
    Literal<?> newValue = newPred.literal();
    Comparator<Object> comparator = (Comparator<Object>) existingValue.comparator();

    switch (existingPred.op()) {
      case LT:
        return newPred.op() == LT
                && comparator.compare(newValue.value(), existingValue.value()) <= 0
            || (newPred.op() == EQ
                && comparator.compare(newValue.value(), existingValue.value()) < 0);
      case LT_EQ:
        return newPred.op() == LT_EQ
                && comparator.compare(newValue.value(), existingValue.value()) <= 0
            || (newPred.op() == EQ
                && comparator.compare(newValue.value(), existingValue.value()) <= 0);
      case GT:
        return newPred.op() == GT
                && comparator.compare(newValue.value(), existingValue.value()) >= 0
            || (newPred.op() == EQ
                && comparator.compare(newValue.value(), existingValue.value()) > 0);
      case GT_EQ:
        return newPred.op() == GT_EQ
                && comparator.compare(newValue.value(), existingValue.value()) >= 0
            || (newPred.op() == EQ
                && comparator.compare(newValue.value(), existingValue.value()) >= 0);
      case STARTS_WITH:
        return newPred.op() == STARTS_WITH
                && ((String) newValue.value()).startsWith((String) existingValue.value())
            || (newPred.op() == EQ
                && ((String) newValue.value()).startsWith((String) existingValue.value()));
      case NOT_STARTS_WITH:
        return newPred.op() == NOT_STARTS_WITH
                && !((String) newValue.value()).startsWith((String) existingValue.value())
            || (newPred.op() == NOT_EQ
                && !((String) newValue.value()).startsWith((String) existingValue.value()));
      case NOT_EQ:
        return newPred.op() == EQ
            && comparator.compare(newValue.value(), existingValue.value()) != 0;
      default:
        return false;
    }
  }

  private static boolean isNotNullForSameRef(Expression existingExpr, Expression newExpr) {
    if (existingExpr instanceof BoundPredicate && newExpr instanceof BoundPredicate) {
      BoundPredicate<?> boundExpr = (BoundPredicate<?>) existingExpr;
      BoundPredicate<?> boundOther = (BoundPredicate<?>) newExpr;
      return boundExpr.op() == NOT_NULL && boundExpr.ref().name().equals(boundOther.ref().name());
    }
    return false;
  }
}
