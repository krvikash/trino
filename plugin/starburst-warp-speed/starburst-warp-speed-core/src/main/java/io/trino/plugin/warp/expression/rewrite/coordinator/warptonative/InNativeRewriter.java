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

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.VariableRewriter.calcDomainPredicateType;
import static io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.VariableRewriter.convertSingleValueToDomain;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

class InNativeRewriter
        implements ExpressionRewriter<WarpCall>
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(ExpressionPatterns.functionName().equalTo(IN_PREDICATE_FUNCTION_NAME.getName()))
            .with(ExpressionPatterns.type().equalTo(BOOLEAN))
            .with(ExpressionPatterns.argumentCount().equalTo(2))
            .with(ExpressionPatterns.argument(1).matching(ExpressionPatterns.call().with(ExpressionPatterns.functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName()))));
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private final PushdownPredicatesStats pushdownPredicatesStats;

    InNativeRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler, PushdownPredicatesStats pushdownPredicatesStats)
    {
        this.nativeExpressionRulesHandler = requireNonNull(nativeExpressionRulesHandler);
        this.pushdownPredicatesStats = requireNonNull(pushdownPredicatesStats);
    }

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    boolean convertIn(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        WarpExpression child0 = warpExpression.getChildren().getFirst();
        boolean validValue;
        if (child0 instanceof WarpVariable) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_NONE);
            validValue = true;
        }
        else {
            validValue = nativeExpressionRulesHandler.rewrite(child0, rewriteContext);
        }
        if (!validValue) {
            return false;
        }
        List<? extends WarpExpression> inConstantValues = warpExpression.getChildren().get(1).getChildren();
        if (inConstantValues.isEmpty()) {
            return false;
        }
        boolean valid = true;
        Type domainType;
        List<Object> functionParams = Collections.emptyList();
        try {
            Function<WarpConstant, Optional<Domain>> convertToDomainFunc = constant -> Optional.of(Domain.singleValue(constant.getType(), constant.getValue()));
            NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
            Type columnType = rewriteContext.columnType();
            if (columnType instanceof MapType mapType) {
                domainType = mapType.getValueType();
            }
            else {
                Type castType = inConstantValues.getFirst().getType();
                boolean isCastExpression = !columnType.equals(castType);
                if (isCastExpression) {
                    functionParams = nativeExpressionBuilder.getFunctionParams();
                    if (castType instanceof VarcharType varcharType) {
                        convertToDomainFunc = constant -> convertSingleValueToDomain((Slice) constant.getValue(), columnType, varcharType.getLength());
                        domainType = columnType;
                    }
                    else {
                        domainType = castType;
                    }
                }
                else {
                    domainType = columnType;
                }
            }
            List<Range> ranges = new ArrayList<>();
            for (WarpExpression value : inConstantValues) {
                Optional<Domain> valueDomain = convertToDomainFunc.apply((WarpConstant) value);
                if (valueDomain.isEmpty()) {
                    pushdownPredicatesStats.incunsupported_functions_native();
                    return false;
                }
                ranges.addAll(valueDomain.get().getValues().getRanges().getOrderedRanges());
            }
            Domain domain;
            if (ranges.isEmpty()) {
                domain = Domain.none(domainType);
            }
            else {
                domain = Domain.create(SortedRangeSet.copyOf(domainType, ranges), false);
            }
            nativeExpressionBuilder.domain(domain)
                    .predicateType(calcDomainPredicateType(domain))
                    .collectNulls(domain.isNullAllowed())
                    .functionParams(functionParams);
        }
        catch (Exception e) {
            logger.debug("failed to convertInFunction. error=%s, warpExpression=%s", e, warpExpression);
            pushdownPredicatesStats.incunsupported_functions_native();
            valid = false;
        }
        return valid;
    }
}
