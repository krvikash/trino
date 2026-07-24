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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.base.expression.ConnectorExpressions.extractVariables;
import static io.trino.spi.expression.Constant.FALSE;
import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public final class ConnectorExpressionUtil
{
    private ConnectorExpressionUtil() {}

    public record ExpressionAndAssignments(
            @JsonProperty("expression") ConnectorExpression expression,
            @JsonProperty("assignments") Map<String, ColumnHandle> assignments)
    {
        public static final ExpressionAndAssignments TRUE = new ExpressionAndAssignments(Constant.TRUE, emptyMap());
        public static final ExpressionAndAssignments FALSE = new ExpressionAndAssignments(Constant.FALSE, emptyMap());

        public ExpressionAndAssignments
        {
            requireNonNull(expression, "expression is null");
            assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        }
    }

    /**
     * Extracts all distinct variable names from {@code expression}
     */
    public static Set<String> extractVariableNames(ConnectorExpression expression)
    {
        return extractVariables(expression).stream()
                .map(Variable::getName)
                .collect(toSet());
    }

    public static ExpressionAndAssignments and(ExpressionAndAssignments... expressionAndAssignments)
    {
        return and(Arrays.asList(expressionAndAssignments));
    }

    public static ExpressionAndAssignments and(List<ExpressionAndAssignments> expressionAndAssignments)
    {
        if (expressionAndAssignments.stream().anyMatch(expression -> FALSE.equals(expression.expression()))) {
            return ExpressionAndAssignments.FALSE;
        }
        List<ExpressionAndAssignments> nonTrivialExpressions = expressionAndAssignments.stream()
                .filter(expression -> !TRUE.equals(expression.expression()))
                .toList();
        if (nonTrivialExpressions.isEmpty()) {
            return ExpressionAndAssignments.TRUE;
        }
        return combineWithLogicalOperator(nonTrivialExpressions, ConnectorExpressions::and);
    }

    public static ExpressionAndAssignments or(ExpressionAndAssignments... expressionAndAssignments)
    {
        return or(Arrays.asList(expressionAndAssignments));
    }

    public static ExpressionAndAssignments or(List<ExpressionAndAssignments> expressionAndAssignments)
    {
        if (expressionAndAssignments.stream().anyMatch(expression -> TRUE.equals(expression.expression()))) {
            return ExpressionAndAssignments.TRUE;
        }
        List<ExpressionAndAssignments> nonTrivialExpressions = expressionAndAssignments.stream()
                .filter(expression -> !FALSE.equals(expression.expression()))
                .toList();
        if (nonTrivialExpressions.isEmpty()) {
            return ExpressionAndAssignments.FALSE;
        }
        return combineWithLogicalOperator(nonTrivialExpressions, ConnectorExpressions::or);
    }

    private static ExpressionAndAssignments combineWithLogicalOperator(
            List<ExpressionAndAssignments> expressionAndAssignments,
            Function<List<ConnectorExpression>, ConnectorExpression> combiner)
    {
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

        ConnectorExpression expression = combiner.apply(expressions);
        Set<String> variableNames = extractVariableNames(expression);
        Map<String, ColumnHandle> prunedAssignments = Maps.filterEntries(
                unionAssignments,
                entry -> variableNames.contains(entry.getKey()));
        return new ExpressionAndAssignments(expression, prunedAssignments);
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
