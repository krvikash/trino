/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.expression.rewrite.coordinator.warptonative;

import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

import static io.trino.plugin.warp.type.TypeUtils.isBigIntegerType;
import static io.trino.plugin.warp.type.TypeUtils.isIntegerType;
import static io.trino.plugin.warp.type.TypeUtils.isSmallIntType;
import static io.trino.plugin.warp.type.TypeUtils.isTinyIntType;

abstract class BaseOperatorRewriter
        implements ExpressionRewriter<WarpCall>
{
    static final BiFunction<Type, Object, ValueSet> EQUAL_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.equal(type, value));
    static final BiFunction<Type, Object, ValueSet> NOT_EQUAL_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value));
    static final BiFunction<Type, Object, ValueSet> GREATER_THAN_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.greaterThan(type, value));
    static final BiFunction<Type, Object, ValueSet> GREATER_THAN_OR_EQUAL_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.greaterThanOrEqual(type, value));
    static final BiFunction<Type, Object, ValueSet> LESS_THAN_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.lessThan(type, value));
    static final BiFunction<Type, Object, ValueSet> LESS_THAN_OR_EQUAL_FUNCTION = (type, value) -> ValueSet.ofRanges(Range.lessThanOrEqual(type, value));
    final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    final PushdownPredicatesStats pushdownPredicatesStats;

    BaseOperatorRewriter(
            NativeExpressionRulesHandler nativeExpressionRulesHandler,
            PushdownPredicatesStats pushdownPredicatesStats)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
        this.pushdownPredicatesStats = pushdownPredicatesStats;
    }

    boolean greaterThan(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(
                warpExpression,
                rewriteContext,
                GREATER_THAN_FUNCTION);
    }

    boolean greaterThanOrEqual(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(
                warpExpression,
                rewriteContext,
                GREATER_THAN_OR_EQUAL_FUNCTION);
    }

    boolean equal(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        WarpConstant warpConstant = ((WarpConstant) warpExpression.getChildren().get(1));
        Type constantType = warpConstant.getType();
        if (constantType.equals(BooleanType.BOOLEAN) && Boolean.FALSE.equals(warpConstant.getValue())) {
            nativeExpressionRulesHandler.countUnsupported(rewriteContext);
            return false;
        }
        return convert(
                warpExpression,
                rewriteContext,
                EQUAL_FUNCTION);
    }

    boolean notEqual(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        Type constantType = ((WarpConstant) warpExpression.getChildren().get(1)).getType();
        if (constantType.equals(BooleanType.BOOLEAN)) {
            nativeExpressionRulesHandler.countUnsupported(rewriteContext);
            return false;
        }
        // NaN <> value is true but lies outside both open ranges, so a floating-point result stays with Trino unless the column is an integer that cannot produce NaN
        boolean floatingPointConstant = constantType.equals(DoubleType.DOUBLE) || constantType.equals(RealType.REAL);
        if (floatingPointConstant && !isIntegerFamily(rewriteContext.columnType())) {
            nativeExpressionRulesHandler.countUnsupported(rewriteContext);
            return false;
        }
        return convert(
                warpExpression,
                rewriteContext,
                NOT_EQUAL_FUNCTION);
    }

    private static boolean isIntegerFamily(Type type)
    {
        return isTinyIntType(type) || isSmallIntType(type) || isIntegerType(type) || isBigIntegerType(type);
    }

    boolean lessThanOrEqual(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(
                warpExpression,
                rewriteContext,
                LESS_THAN_OR_EQUAL_FUNCTION);
    }

    boolean lessThan(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(
                warpExpression,
                rewriteContext,
                LESS_THAN_FUNCTION);
    }

    abstract boolean convert(
            WarpExpression warpExpression,
            RewriteContext rewriteContext,
            BiFunction<Type, Object, ValueSet> valueSetFunction);
}
