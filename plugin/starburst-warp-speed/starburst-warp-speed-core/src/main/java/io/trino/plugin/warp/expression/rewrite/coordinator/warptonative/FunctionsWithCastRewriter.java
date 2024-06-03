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
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;

import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.functionName;

public class FunctionsWithCastRewriter
        implements ExpressionRewriter<WarpCall>
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(functionName().matching(functionName -> SupportedFunctions.DATE_FUNCTIONS.contains(new FunctionName(functionName))))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(x -> x instanceof WarpCall warpCall && warpCall.getFunctionName().equals(StandardFunctions.CAST_FUNCTION_NAME.getName())));
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;

    public FunctionsWithCastRewriter(NativeExpressionRulesHandler nativeExpressionRulesHandler)
    {
        this.nativeExpressionRulesHandler = nativeExpressionRulesHandler;
    }

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    public boolean dayOfWeek(WarpExpression warpExpression, RewriteContext parentContext)
    {
        WarpCall castFunction = (WarpCall) warpExpression.getChildren().get(0);
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_WEEK);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean day(WarpExpression warpExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY);
        WarpCall castFunction = (WarpCall) warpExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean dayOfYear(WarpExpression warpExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_YEAR);
        WarpCall castFunction = (WarpCall) warpExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean week(WarpExpression warpExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_WEEK);
        WarpCall castFunction = (WarpCall) warpExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }

    public boolean yearOfWeek(WarpExpression warpExpression, RewriteContext parentContext)
    {
        parentContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_YEAR_OF_WEEK);
        WarpCall castFunction = (WarpCall) warpExpression.getChildren().get(0);
        return nativeExpressionRulesHandler.rewrite(castFunction, parentContext);
    }
}
