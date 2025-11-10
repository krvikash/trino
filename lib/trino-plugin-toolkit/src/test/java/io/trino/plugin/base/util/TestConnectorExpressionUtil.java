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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.or;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

final class TestConnectorExpressionUtil
{
    @Test
    void testRemapSymbolsVariable()
    {
        ConnectorExpression result = ConnectorExpressionUtil.remapSymbols(
                variable("a"),
                Map.of("a", "b"));

        assertThat(result).isEqualTo(variable("b"));
    }

    @Test
    void testRemapSymbolsNestedFieldDereference()
    {
        ConnectorExpression result = ConnectorExpressionUtil.remapSymbols(
                nestedFieldDereference("a"),
                Map.of("a", "b"));

        assertThat(result).isEqualTo(nestedFieldDereference("b"));
    }

    @Test
    void testRemapSymbolsRecursiveCall()
    {
        ConnectorExpression result = ConnectorExpressionUtil.remapSymbols(
                lessThanOrEqual(
                        lessThanOrEqual(variable("x"), variable("y")),
                        fieldDereference("z")),
                Map.of("x", "a", "y", "b", "z", "c"));

        assertThat(result).isEqualTo(
                lessThanOrEqual(
                        lessThanOrEqual(variable("a"), variable("b")),
                        fieldDereference("c")));
    }

    @Test
    void testAndEmpty()
    {
        assertThat(ConnectorExpressionUtil.and(ImmutableList.of()))
                .isEqualTo(new ExpressionAndAssignments(TRUE, emptyMap()));
    }

    @Test
    void testAndSingle()
    {
        ExpressionAndAssignments expressionAndAssignments = new ExpressionAndAssignments(
                isNull(variable("a")),
                Map.of("a", new TestingColumnHandle("c")));

        assertThat(ConnectorExpressionUtil.and(ImmutableList.of(expressionAndAssignments)))
                .isEqualTo(expressionAndAssignments);
    }

    @Test
    void testAndNoConflicts()
    {
        Call expression1 = isNull(variable("a"));
        Call expression2 = isNull(variable("b"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("c1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("c2");
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                new ExpressionAndAssignments(expression1, Map.of("a", columnHandle1)),
                new ExpressionAndAssignments(expression2, Map.of("b", columnHandle2)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(expression1, expression2),
                        Map.of("a", columnHandle1, "b", columnHandle2)));
    }

    @Test
    void testAndSameExpressionAndAssignments()
    {
        Call expression = isNull(variable("a"));
        ColumnHandle columnHandle = new TestingColumnHandle("c1");
        ExpressionAndAssignments expressionAndAssignments = new ExpressionAndAssignments(expression, Map.of("a", columnHandle));
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(expressionAndAssignments, expressionAndAssignments);

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(expression, expression),
                        Map.of("a", columnHandle)));
    }

    @Test
    void testOrSameExpressionAndAssignments()
    {
        Call expression = isNull(variable("a"));
        ColumnHandle columnHandle = new TestingColumnHandle("c1");
        ExpressionAndAssignments expressionAndAssignments = new ExpressionAndAssignments(expression, Map.of("a", columnHandle));
        ExpressionAndAssignments result = ConnectorExpressionUtil.or(expressionAndAssignments, expressionAndAssignments);

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        or(expression, expression),
                        Map.of("a", columnHandle)));
    }

    @Test
    void testDifferentSymbolForTheSameColumn()
    {
        Call expressionA = isNull(variable("a"));
        Call expressionB = isNull(variable("b"));
        ColumnHandle columnHandle = new TestingColumnHandle("column1");
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle)),
                new ExpressionAndAssignments(expressionB, Map.of("b", columnHandle)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(expressionA, expressionA),
                        Map.of("a", columnHandle)));
    }

    @Test
    void testAndSymbolWithTwoConflicts()
    {
        Call expression = isNull(variable("a"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle1)),
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle2)),
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle3)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(
                                expression,
                                isNull(variable("a_0")),
                                isNull(variable("a_1"))),
                        Map.of(
                                "a", columnHandle1,
                                "a_0", columnHandle2,
                                "a_1", columnHandle3)));
    }

    @Test
    void testOrSymbolWithTwoConflicts()
    {
        Call expression = isNull(variable("a"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ExpressionAndAssignments result = ConnectorExpressionUtil.or(
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle1)),
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle2)),
                new ExpressionAndAssignments(expression, Map.of("a", columnHandle3)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        or(
                                expression,
                                isNull(variable("a_0")),
                                isNull(variable("a_1"))),
                        Map.of(
                                "a", columnHandle1,
                                "a_0", columnHandle2,
                                "a_1", columnHandle3)));
    }

    @Test
    void testSymbolWithConflictAfterConflict()
    {
        Call expressionA = isNull(variable("a"));
        Call expressionA0 = isNull(variable("a_0"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle1)),
                new ExpressionAndAssignments(expressionA0, Map.of("a_0", columnHandle2)),
                new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle3)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(
                                expressionA,
                                expressionA0,
                                isNull(variable("a_1"))),
                        Map.of(
                                "a", columnHandle1,
                                "a_0", columnHandle2,
                                "a_1", columnHandle3)));
    }

    @Test
    void testAndDuplicateAssignmentsForTheSameColumnWithConflict()
    {
        Call expression = lessThanOrEqual(variable("a"), variable("b"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                new ExpressionAndAssignments(expression, orderedMapOf("a", columnHandle1, "b", columnHandle1)),
                new ExpressionAndAssignments(expression, orderedMapOf("a", columnHandle2, "b", columnHandle2)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(
                                lessThanOrEqual(variable("a"), variable("a")),
                                lessThanOrEqual(variable("a_0"), variable("a_0"))),
                        Map.of(
                                "a", columnHandle1,
                                "a_0", columnHandle2)));
    }

    @Test
    void testOrWithTwoConflictsAndACommonColumn()
    {
        Call expression = lessThanOrEqual(variable("a"), variable("b"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ExpressionAndAssignments result = ConnectorExpressionUtil.or(
                new ExpressionAndAssignments(expression, orderedMapOf("a", columnHandle1, "b", columnHandle2)),
                new ExpressionAndAssignments(expression, orderedMapOf("a", columnHandle2, "b", columnHandle3)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        or(
                                lessThanOrEqual(variable("a"), variable("b")),
                                lessThanOrEqual(variable("b"), variable("b_0"))),
                        Map.of(
                                "a", columnHandle1,
                                "b", columnHandle2,
                                "b_0", columnHandle3)));
    }

    @Test
    void testAndOfOrs()
    {
        Call expressionA = isNull(variable("a"));
        Call expressionB = isNull(variable("b"));
        Call expressionC = isNull(variable("c"));
        Call expressionD = isNull(variable("d"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ColumnHandle columnHandle4 = new TestingColumnHandle("column4");

        ExpressionAndAssignments result =
                ConnectorExpressionUtil.and(
                        ConnectorExpressionUtil.or(
                                new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle1)),
                                new ExpressionAndAssignments(expressionB, Map.of("b", columnHandle2))),
                        ConnectorExpressionUtil.or(
                                new ExpressionAndAssignments(expressionC, Map.of("c", columnHandle3)),
                                new ExpressionAndAssignments(expressionD, Map.of("d", columnHandle4))));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(
                                or(expressionA, expressionB),
                                or(expressionC, expressionD)),
                        Map.of(
                                "a", columnHandle1,
                                "b", columnHandle2,
                                "c", columnHandle3,
                                "d", columnHandle4)));
    }

    @Test
    void testOrOfAnds()
    {
        Call expressionA = isNull(variable("a"));
        Call expressionB = isNull(variable("b"));
        Call expressionC = isNull(variable("c"));
        Call expressionD = isNull(variable("d"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");
        ColumnHandle columnHandle4 = new TestingColumnHandle("column4");

        ExpressionAndAssignments result = ConnectorExpressionUtil.or(
                ConnectorExpressionUtil.and(
                        new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle1)),
                        new ExpressionAndAssignments(expressionB, Map.of("b", columnHandle2))),
                ConnectorExpressionUtil.and(
                        new ExpressionAndAssignments(expressionC, Map.of("c", columnHandle3)),
                        new ExpressionAndAssignments(expressionD, Map.of("d", columnHandle4))));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        or(
                                and(expressionA, expressionB),
                                and(expressionC, expressionD)),
                        Map.of(
                                "a", columnHandle1,
                                "b", columnHandle2,
                                "c", columnHandle3,
                                "d", columnHandle4)));
    }

    @Test
    void testNestedOrWithConflict()
    {
        Call expressionA = isNull(variable("a"));
        Call expressionB = isNull(variable("b"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");

        ExpressionAndAssignments result = ConnectorExpressionUtil.and(
                ConnectorExpressionUtil.or(
                        new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle1)),
                        new ExpressionAndAssignments(expressionB, Map.of("b", columnHandle2))),
                new ExpressionAndAssignments(expressionA, Map.of("a", columnHandle3)));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        and(
                                or(expressionA, expressionB),
                                isNull(variable("a_0"))),
                        Map.of(
                                "a", columnHandle1,
                                "b", columnHandle2,
                                "a_0", columnHandle3)));
    }

    @Test
    void testOrOfAndsWithConflicts()
    {
        Call expression = lessThanOrEqual(variable("x"), variable("y"));
        ColumnHandle columnHandle1 = new TestingColumnHandle("column1");
        ColumnHandle columnHandle2 = new TestingColumnHandle("column2");
        ColumnHandle columnHandle3 = new TestingColumnHandle("column3");

        ExpressionAndAssignments result = ConnectorExpressionUtil.or(
                ConnectorExpressionUtil.and(
                        new ExpressionAndAssignments(expression, orderedMapOf("x", columnHandle1, "y", columnHandle2)),
                        new ExpressionAndAssignments(expression, orderedMapOf("x", columnHandle2, "y", columnHandle3))),
                ConnectorExpressionUtil.and(
                        new ExpressionAndAssignments(expression, orderedMapOf("x", columnHandle2, "y", columnHandle3)),
                        new ExpressionAndAssignments(expression, orderedMapOf("x", columnHandle3, "y", columnHandle1))));

        assertThat(result).isEqualTo(
                new ExpressionAndAssignments(
                        or(
                                and(
                                        lessThanOrEqual(variable("x"), variable("y")),
                                        lessThanOrEqual(variable("y"), variable("y_0"))),
                                and(
                                        lessThanOrEqual(variable("y"), variable("y_0")),
                                        lessThanOrEqual(variable("y_0"), variable("x")))),
                        Map.of(
                                "x", columnHandle1,
                                "y", columnHandle2,
                                "y_0", columnHandle3)));
    }

    private static <K, V> Map<K, V> orderedMapOf(K k1, V v1, K k2, V v2)
    {
        Map<K, V> orderedMap = new LinkedHashMap<>();
        orderedMap.put(k1, v1);
        orderedMap.put(k2, v2);
        return orderedMap;
    }

    private static Call isNull(ConnectorExpression argument)
    {
        return new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(argument));
    }

    private static Call lessThanOrEqual(ConnectorExpression left, ConnectorExpression right)
    {
        return new Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(left, right));
    }

    private static Variable variable(String name)
    {
        return new Variable(name, BOOLEAN);
    }

    private static FieldDereference fieldDereference(String variableName)
    {
        return new FieldDereference(BOOLEAN, new Variable(variableName, rowType(field(BOOLEAN))), 0);
    }

    private static FieldDereference nestedFieldDereference(String variableName)
    {
        Variable variable = new Variable(variableName, rowType(field(rowType(field(BOOLEAN)))));
        FieldDereference innerDereference = new FieldDereference(rowType(field(BOOLEAN)), variable, 0);
        return new FieldDereference(BOOLEAN, innerDereference, 0);
    }
}
