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
package io.trino.plugin.singlestore;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.distinct;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.hasFilter;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ImplementCaseSensitiveCountDistinct
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    private final JdbcTypeHandle bigintTypeHandle;

    public ImplementCaseSensitiveCountDistinct(JdbcTypeHandle bigintTypeHandle)
    {
        this.bigintTypeHandle = requireNonNull(bigintTypeHandle, "bigintTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class)
                .with(distinct().equalTo(true))
                .with(hasFilter().equalTo(false))
                .with(functionName().equalTo("count"))
                .with(singleArgument().matching(variable().capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(argument.getName());
        verify(aggregateFunction.getOutputType() == BIGINT);

        boolean isTextual = columnHandle.getColumnType() instanceof CharType || columnHandle.getColumnType() instanceof VarcharType;
        // Use BINARY to enforce case-sensitive on string columns
        String format = isTextual ? "count(DISTINCT BINARY %s)" : "count(DISTINCT %s)";

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(argument).orElseThrow();
        return Optional.of(new JdbcExpression(
                format(format, rewrittenArgument.expression()),
                rewrittenArgument.parameters(),
                bigintTypeHandle));
    }
}
