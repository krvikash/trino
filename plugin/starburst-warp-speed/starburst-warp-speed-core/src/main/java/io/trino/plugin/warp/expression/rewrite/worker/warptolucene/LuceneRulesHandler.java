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
package io.trino.plugin.warp.expression.rewrite.worker.warptolucene;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.matching.Pattern;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;

import java.util.Set;
import java.util.function.BiFunction;

import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.CONTAINS;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.START_WITH;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

public class LuceneRulesHandler
{
    private SetMultimap<String, FunctionRewriter> luceneRules;

    public LuceneRulesHandler() {}

    public void init(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        GeneralRewriter luceneGeneralRewriter = new GeneralRewriter(dispatcherProxiedConnectorTransformer);
        EqualityRewriter equalityRewriter = new EqualityRewriter(this);
        LogicalOperatorRewriter luceneLogicalOperatorRewriter = new LogicalOperatorRewriter(this);
        NotRewriter notRewriter = new NotRewriter(this);
        /*
        InRewriter wraps each value with wildcards, which is inefficient. For now, we've decided to disable this functionality
        luceneRules.put(IN_PREDICATE_FUNCTION_NAME.getName(), new FunctionRewriter(inRewriter.getPattern(), inRewriter::handleIn));
         */
        LuceneVariableRewriter variableRewriter = new LuceneVariableRewriter();
        luceneRules = HashMultimap.create();
        luceneRules.put(IS_NULL_FUNCTION_NAME.getName(), new FunctionRewriter(variableRewriter.getPattern(), variableRewriter::handleIsNull));
        luceneRules.put(NOT_FUNCTION_NAME.getName(), new FunctionRewriter(notRewriter.getPattern(), notRewriter::handleNotLike));
        luceneRules.put(LIKE_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLike));
        luceneRules.put(START_WITH.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleStartsWith));
        luceneRules.put(OR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneLogicalOperatorRewriter.getPattern(), luceneLogicalOperatorRewriter::handleOr));
        luceneRules.put(AND_FUNCTION_NAME.getName(), new FunctionRewriter(luceneLogicalOperatorRewriter.getPattern(), luceneLogicalOperatorRewriter::handleAnd));
        luceneRules.put(CONTAINS.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleContains));
        luceneRules.put(NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(equalityRewriter.getPattern(), equalityRewriter::handleNotEqual));
        luceneRules.put(NOT_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleNotEqual));
        luceneRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleEqual));
        luceneRules.put(EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(equalityRewriter.getPattern(), equalityRewriter::handleEqual));
        luceneRules.put(LESS_THAN_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLessThan));
        luceneRules.put(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::handleLessThanOrEqual));
        luceneRules.put(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::greateThan));
        luceneRules.put(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(), new FunctionRewriter(luceneGeneralRewriter.getPattern(), luceneGeneralRewriter::greatThanOrEqual));
    }

    public boolean rewrite(
            WarpExpression warpExpression,
            LuceneRewriteContext context)
    {
        if (warpExpression instanceof WarpCall warpCall) {
            String functionName = warpCall.getFunctionName();
            Set<FunctionRewriter> warpExpressionRules = luceneRules.get(functionName);
            boolean isValid = false;
            for (FunctionRewriter rule : warpExpressionRules) {
                if (rule.pattern().matches(warpExpression, null)) {
                    isValid = rule.rewriteCallback().apply(warpExpression, context);
                    break;
                }
            }
            return isValid;
        }
        return false;
    }

    private record FunctionRewriter(
            @SuppressWarnings("unused") Pattern<WarpCall> pattern,
            @SuppressWarnings("unused") BiFunction<WarpExpression, LuceneRewriteContext, Boolean> rewriteCallback) {}
}
