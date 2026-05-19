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
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.function.BiFunction;

import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.VariableRewriter.calcPredicateType;
import static io.trino.plugin.warp.type.TypeUtils.isWarmBasicSupported;

class VariableAndConstantRewriter
        extends BaseOperatorRewriter
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof WarpVariable))
            .with(argument(1).matching(x -> x instanceof WarpConstant));

    VariableAndConstantRewriter(
            NativeExpressionRulesHandler nativeExpressionRulesHandler,
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
    boolean convert(WarpExpression warpExpression, RewriteContext rewriteContext, BiFunction<Type, Object, Range> rangeBiFunction)
    {
        if (rewriteContext.nativeExpressionBuilder().getDomain() != null) {
            pushdownPredicatesStats.incunsupported_functions_composite();
            return false;
        }
        WarpConstant warpConstant = ((WarpConstant) warpExpression.getChildren().get(1));
        Type constantType = warpConstant.getType();
        String functionName = ((WarpCall) warpExpression).getFunctionName();
        PredicateType predicateType = calcPredicateType(constantType, functionName);

        Domain domain = convertConstantToDomain(warpConstant, rangeBiFunction);
        NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
        nativeExpressionBuilder.domain(domain)
                .predicateType(predicateType)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .collectNulls(domain.isNullAllowed());
        return true;
    }

    public boolean elementAt(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean res = false;
        if (warpExpression.getChildren().get(0).getType() instanceof MapType mapType && isWarmBasicSupported(mapType.getValueType())) {
            WarpConstant warpConstant = (WarpConstant) warpExpression.getChildren().get(1);
            NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
            nativeExpressionBuilder
                    .functionType(FunctionType.FUNCTION_TYPE_TRANSFORMED)
                    .transformedColumn(new TransformFunction(TransformFunction.TransformType.ELEMENT_AT, List.of(warpConstant)));
            res = true;
        }
        return res;
    }

    public boolean jsonExtractScalar(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        WarpSliceConstant warpSliceConstant = (WarpSliceConstant) warpExpression.getChildren()
                .stream()
                .filter(warpExpr -> warpExpr instanceof WarpSliceConstant)
                .findFirst()
                .orElseThrow();
        rewriteContext
                .nativeExpressionBuilder()
                .functionType(FunctionType.FUNCTION_TYPE_TRANSFORMED)
                .transformedColumn(new TransformFunction(
                        TransformFunction.TransformType.JSON_EXTRACT_SCALAR,
                        List.of(new WarpPrimitiveConstant(warpSliceConstant.getValue().toStringUtf8(), VarcharType.VARCHAR))));
        return true;
    }
}
