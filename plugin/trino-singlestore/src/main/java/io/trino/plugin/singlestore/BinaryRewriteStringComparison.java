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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.singlestore.SingleStoreSessionProperties.isEnableStringPushdownWithBinary;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class BinaryRewriteStringComparison
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> LEFT_OPERAND = newCapture();
    private static final Capture<ConnectorExpression> RIGHT_OPERAND = newCapture();
    private static final ImmutableSet<FunctionName> COMPARISON_OPERATOR_NAMES = Stream.of(ComparisonOperator.values())
            .map(ComparisonOperator::getFunctionName)
            .collect(toImmutableSet());
    private static final Pattern<Call> PATTERN = call()
            .with(type().equalTo(BOOLEAN))
            .with(functionName().matching(COMPARISON_OPERATOR_NAMES::contains))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(expression().with(type().matching(type -> type instanceof VarcharType || type instanceof CharType)).capturedAs(LEFT_OPERAND)))
            .with(argument(1).matching(expression().with(type().matching(type -> type instanceof VarcharType || type instanceof CharType)).capturedAs(RIGHT_OPERAND)));
    private static final String SINGLE_STORE_IDENTICAL_OPERATOR = "<=>";

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!isEnableStringPushdownWithBinary(context.getSession())) {
            return Optional.empty();
        }
        ComparisonOperator comparison = ComparisonOperator.forFunctionName(expression.getFunctionName());
        ConnectorExpression leftOperand = captures.get(LEFT_OPERAND);
        ConnectorExpression rightOperand = captures.get(RIGHT_OPERAND);
        if (!(leftOperand instanceof Variable || leftOperand instanceof Constant)) {
            return Optional.empty();
        }
        if (!(rightOperand instanceof Variable || rightOperand instanceof Constant)) {
            return Optional.empty();
        }
        return context.defaultRewrite(leftOperand).flatMap(first ->
                context.defaultRewrite(rightOperand).map(second ->
                        new ParameterizedExpression(
                                "BINARY (%s) %s (%s)".formatted(first.expression(), mapOperator(comparison), second.expression()),
                                ImmutableList.<QueryParameter>builder()
                                        .addAll(first.parameters())
                                        .addAll(second.parameters())
                                        .build())));
    }

    private static String mapOperator(ComparisonOperator comparison)
    {
        if (comparison == ComparisonOperator.IDENTICAL) {
            return SINGLE_STORE_IDENTICAL_OPERATOR;
        }
        else {
            return comparison.getOperator();
        }
    }
}
