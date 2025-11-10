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
package io.trino.plugin.base.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionUtil
{
    private ConnectorExpressionUtil() {}

    public record ExpressionAndAssignments(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        public ExpressionAndAssignments
        {
            requireNonNull(expression, "expression is null");
            assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        }
    }

    public static ExpressionAndAssignments and(ExpressionAndAssignments... expressionAndAssignments)
    {
        return and(Arrays.asList(expressionAndAssignments));
    }

    public static ExpressionAndAssignments and(List<ExpressionAndAssignments> expressionAndAssignments)
    {
        return combineWithLogicalOperator(expressionAndAssignments, ConnectorExpressions::and);
    }

    public static ExpressionAndAssignments or(ExpressionAndAssignments... expressionAndAssignments)
    {
        return or(Arrays.asList(expressionAndAssignments));
    }

    public static ExpressionAndAssignments or(List<ExpressionAndAssignments> expressionAndAssignments)
    {
        return combineWithLogicalOperator(expressionAndAssignments, ConnectorExpressions::or);
    }

    private static ExpressionAndAssignments combineWithLogicalOperator(
            List<ExpressionAndAssignments> expressionAndAssignments,
            Function<List<ConnectorExpression>, ConnectorExpression> combiner)
    {
        if (expressionAndAssignments.isEmpty()) {
            return new ExpressionAndAssignments(TRUE, emptyMap());
        }
        if (expressionAndAssignments.size() == 1) {
            return expressionAndAssignments.getFirst();
        }

        Map<String, ColumnHandle> unionAssignments = new LinkedHashMap<>();
        Map<ColumnHandle, String> unionReversedAssignments = new HashMap<>();
        List<ConnectorExpression> expressions = new ArrayList<>();
        for (ExpressionAndAssignments current : expressionAndAssignments) {
            Map<String, String> symbolMapping = new HashMap<>();
            for (Map.Entry<String, ColumnHandle> assignment : current.assignments().entrySet()) {
                String symbol = assignment.getKey();
                ColumnHandle columnHandle = assignment.getValue();
                if (columnHandle.equals(unionAssignments.get(symbol))) {
                    // equivalent assignment already exists
                    continue;
                }
                if (unionReversedAssignments.containsKey(columnHandle)) {
                    // reuse existing symbol for this columnHandle
                    symbolMapping.put(symbol, unionReversedAssignments.get(columnHandle));
                    continue;
                }
                if (unionAssignments.containsKey(symbol)) {
                    // conflicting assignment detected - generate a unique symbol
                    String newSymbol = symbol + "_";
                    int id = 0;
                    while (unionAssignments.containsKey(newSymbol + id)) {
                        id++;
                    }
                    newSymbol += id;
                    symbolMapping.put(symbol, newSymbol);
                    unionAssignments.put(newSymbol, columnHandle);
                    unionReversedAssignments.put(columnHandle, newSymbol);
                }
                else {
                    unionAssignments.put(symbol, columnHandle);
                    unionReversedAssignments.put(columnHandle, symbol);
                }
            }
            expressions.add(symbolMapping.isEmpty() ? current.expression() : remapSymbols(current.expression(), symbolMapping));
        }

        return new ExpressionAndAssignments(combiner.apply(expressions), unionAssignments);
    }

    static ConnectorExpression remapSymbols(ConnectorExpression expression, Map<String, String> symbolMapping)
    {
        if (expression instanceof Variable variable) {
            String mappedName = symbolMapping.getOrDefault(variable.getName(), variable.getName());
            return new Variable(mappedName, variable.getType());
        }

        if (expression instanceof Constant) {
            return expression;
        }

        if (expression instanceof FieldDereference dereference) {
            ConnectorExpression newTarget = remapSymbols(dereference.getTarget(), symbolMapping);
            return new FieldDereference(dereference.getType(), newTarget, dereference.getField());
        }

        if (expression instanceof Call call) {
            List<ConnectorExpression> newArguments = call.getArguments().stream()
                    .map(argument -> remapSymbols(argument, symbolMapping))
                    .collect(Collectors.toList());
            return new Call(call.getType(), call.getFunctionName(), newArguments);
        }

        throw new IllegalArgumentException("Unknown ConnectorExpression type: " + expression.getClass());
    }
}
