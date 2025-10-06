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
package io.trino.plugin.jdbc.expression;

import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;

import java.util.Locale;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRewriteDateTrunc
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Constant> UNIT = newCapture();
    private static final Capture<Variable> VALUE = newCapture();

    public enum Unit
    {
        MILLISECOND("millisecond"),
        SECOND("second"),
        MINUTE("minute"),
        HOUR("hour"),
        DAY("day"),
        WEEK("week"),
        MONTH("month"),
        QUARTER("quarter"),
        YEAR("year"),;

        private final String value;

        Unit(String value)
        {
            this.value = requireNonNull(value, "value is null").toLowerCase(Locale.ENGLISH);
        }

        public String getValue()
        {
            return value;
        }

        public static Unit fromString(String value)
        {
            for (Unit unit : Unit.values()) {
                if (unit.getValue().equalsIgnoreCase(value)) {
                    return unit;
                }
            }
            throw new IllegalArgumentException("Unknown unit: " + value);
        }
    }

    protected abstract boolean pushdownSupported(JdbcTypeHandle jdbcTypeHandle, Unit unit);

    protected String buildDateTrunc(@SuppressWarnings("unused") JdbcTypeHandle typeHandle, ParameterizedExpression expression, Unit unit)
    {
        return "date_trunc('%s', %s)".formatted(unit, expression.expression());
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return call()
                .with(functionName().equalTo(new FunctionName("date_trunc")))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(constant().capturedAs(UNIT)))
                .with(argument(1).matching(variable().capturedAs(VALUE)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Constant unit = captures.get(UNIT);
        Variable variable = captures.get(VALUE);

        Optional<ParameterizedExpression> value = context.rewriteExpression(variable);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        Slice unitSlice = (Slice) unit.getValue();
        if (unitSlice == null) {
            return Optional.empty();
        }
        JdbcTypeHandle jdbcTypeHandle = ((JdbcColumnHandle) context.getAssignment(variable.getName())).getJdbcTypeHandle();
        Unit unitValue = Unit.fromString(unitSlice.toStringUtf8());
        if (!pushdownSupported(jdbcTypeHandle, unitValue)) {
            return Optional.empty();
        }
        return Optional.of(new JdbcExpression(
                buildDateTrunc(jdbcTypeHandle, value.get(), unitValue),
                value.get().parameters(),
                jdbcTypeHandle));
    }
}
