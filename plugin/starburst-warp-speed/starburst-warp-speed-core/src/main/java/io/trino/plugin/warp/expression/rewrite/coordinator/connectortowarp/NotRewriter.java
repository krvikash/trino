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
package io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionPatterns;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.spi.expression.Call;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;

class NotRewriter
        implements ConnectorExpressionRule<Call, WarpExpression>

{
    private static final Pattern<Call> PATTERN = ConnectorExpressionPatterns.call()
            .with(ConnectorExpressionPatterns.argumentCount().equalTo(1))
            .with(ConnectorExpressionPatterns.argument(0).matching(x -> x instanceof Call))
            .with(ConnectorExpressionPatterns.argument(0).matching(x -> ((Call) x).getFunctionName().equals(LIKE_FUNCTION_NAME)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<WarpExpression> rewrite(Call expression, Captures captures, RewriteContext<WarpExpression> context)
    {
        Optional<WarpExpression> warpExpression = context.defaultRewrite(expression.getChildren().get(0));
        return warpExpression.map(value -> new WarpCall(NOT_FUNCTION_NAME.getName(), List.of(value), expression.getType()));
    }
}
