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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BooleanType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.documentOf;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;

public class RewriteNot
        implements ConnectorExpressionRule<Call, MongoExpression>
{
    private final Pattern<Call> pattern;

    public RewriteNot()
    {
        this.pattern = call()
                .with(functionName().equalTo(NOT_FUNCTION_NAME))
                .with(type().matching(BooleanType.class::isInstance))
                .with(argumentCount().equalTo(1));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<MongoExpression> rewrite(Call call, Captures captures, RewriteContext<MongoExpression> context)
    {
        List<ConnectorExpression> arguments = call.getArguments();

        Optional<MongoExpression> rewritten = context.defaultRewrite(arguments.get(0));
        if (rewritten.isEmpty()) {
            return Optional.empty();
        }
        Object value = rewritten.get().expression();
        if (arguments.get(0) instanceof Variable) {
            value = "$" + value;
        }
        return Optional.of(new MongoExpression(documentOf("$not", Arrays.asList(value))));
    }
}
