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

import io.trino.matching.Pattern;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;

import java.util.function.BiFunction;

import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.VariableRewriter.calcPredicateType;

class CallAndConstantRewriter
        extends BaseOperatorRewriter
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof WarpCall))
            .with(argument(1).matching(x -> x instanceof WarpConstant));

    CallAndConstantRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler,
            PushdownPredicatesStats pushdownPredicatesStats)
    {
        super(nativeExpressionRulesHandler, pushdownPredicatesStats);
    }

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    @Override
    boolean convert(WarpExpression warpExpression,
            RewriteContext rewriteContext,
            BiFunction<Type, Object, Range> rangeBiFunction)
    {
        if (rewriteContext.nativeExpressionBuilder().getDomain() != null) {
            pushdownPredicatesStats.incunsupported_functions_composite();
            //currently, not supported complex expression. etc: where (ceil(c1) > 5) = false
            return false;
        }
        WarpConstant varadaConstant = ((WarpConstant) warpExpression.getChildren().get(1));
        Type constantType = varadaConstant.getType();
        String functionName = ((WarpCall) warpExpression).getFunctionName();
        PredicateType predicateType = calcPredicateType(constantType, functionName);

        Domain domain = convertConstantToDomain(varadaConstant, rangeBiFunction);
        NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
        nativeExpressionBuilder.domain(domain)
                .predicateType(predicateType)
                .collectNulls(domain.isNullAllowed());
        return nativeExpressionRulesHandler.rewrite(warpExpression.getChildren().get(0), rewriteContext);
    }
}
