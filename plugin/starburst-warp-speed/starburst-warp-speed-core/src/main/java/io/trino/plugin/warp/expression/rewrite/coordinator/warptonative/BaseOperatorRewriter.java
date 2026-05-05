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
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

abstract class BaseOperatorRewriter
        implements ExpressionRewriter<WarpCall>
{
    static final BiFunction<Type, Object, Range> EQUAL_FUNCTION = Range::equal;
    static final BiFunction<Type, Object, Range> GREATER_THAN_FUNCTION = Range::greaterThan;
    static final BiFunction<Type, Object, Range> GREATER_THAN_OR_EQUAL_FUNCTION = Range::greaterThanOrEqual;
    static final BiFunction<Type, Object, Range> LESS_THAN_FUNCTION = Range::lessThan;
    static final BiFunction<Type, Object, Range> LESS_THAN_OR_EQUAL_FUNCTION = Range::lessThanOrEqual;
    final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    final PushdownPredicatesStats pushdownPredicatesStats;

    BaseOperatorRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler,
            PushdownPredicatesStats pushdownPredicatesStats)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
        this.pushdownPredicatesStats = pushdownPredicatesStats;
    }

    boolean greaterThan(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(warpExpression,
                rewriteContext,
                GREATER_THAN_FUNCTION);
    }

    boolean greaterThanOrEqual(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(warpExpression,
                rewriteContext,
                GREATER_THAN_OR_EQUAL_FUNCTION);
    }

    boolean equal(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        WarpConstant warpConstant = ((WarpConstant) warpExpression.getChildren().get(1));
        Type constantType = warpConstant.getType();
        if (constantType == BooleanType.BOOLEAN && warpConstant.getValue() == (Object) false) {
            rewriteContext.customStats().compute("unsupported_functions_native", (_, value) -> value == null ? 1L : value + 1);
            pushdownPredicatesStats.incunsupported_functions_native();
            return false;
        }
        return convert(warpExpression,
                rewriteContext,
                EQUAL_FUNCTION);
    }

    boolean lessThanOrEqual(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(warpExpression,
                rewriteContext,
                LESS_THAN_OR_EQUAL_FUNCTION);
    }

    boolean lessThan(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        return convert(warpExpression,
                rewriteContext,
                LESS_THAN_FUNCTION);
    }

    abstract boolean convert(WarpExpression warpExpression,
            RewriteContext rewriteContext,
            BiFunction<Type, Object, Range> rangeBiFunction);
}
