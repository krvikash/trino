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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.Fallibility.INFALLIBLE;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFallibility
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final Reference A = new Reference(BIGINT, "a");
    private static final Reference B = new Reference(BIGINT, "b");
    private static final Reference C = new Reference(BOOLEAN, "c");
    private static final Reference D = new Reference(DATE, "d");
    private static final Reference E = new Reference(BOOLEAN, "e");
    private static final Reference T = new Reference(createTimestampType(9), "t");
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(
            Symbol.from(A), 0,
            Symbol.from(B), 1,
            Symbol.from(C), 2,
            Symbol.from(D), 3,
            Symbol.from(E), 4,
            Symbol.from(T), 5);

    @Test
    void testComparison()
    {
        GpuExpression expression = compile(comparison(ComparisonOperator.GREATER_THAN, A, bigint(5)));
        assertThat(expression.fallibility()).isEqualTo(INFALLIBLE);
        assertThat(expression.nullOnNullInputs()).isTrue();
    }

    @Test
    void testDivide()
    {
        assertThat(compile(divide(A, B)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(divide(bigint(10), B)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(divide(bigint(10), bigint(0))).fallibility()).isEqualTo(FALLIBLE);
    }

    @Test
    void testDivideByConstant()
    {
        assertThat(compile(divide(A, bigint(2))).fallibility()).isEqualTo(INFALLIBLE);
        assertThat(compile(divide(A, bigint(-1))).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(modulo(A, bigint(-1))).fallibility()).isEqualTo(INFALLIBLE);
        assertThat(compile(modulo(A, bigint(0))).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
    }

    @Test
    void testDivideByCoalesce()
    {
        GpuExpression divisor = compile(new Coalesce(B, bigint(0)));
        assertThat(divisor.fallibility()).isEqualTo(INFALLIBLE);
        assertThat(divisor.nullOnNullInputs()).isFalse();

        assertThat(compile(divide(bigint(10), new Coalesce(B, bigint(0)))).fallibility()).isEqualTo(FALLIBLE);
    }

    @Test
    void testCoalesce()
    {
        GpuExpression expression = compile(new Coalesce(A, divide(bigint(10), B)));
        assertThat(expression.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(expression.nullOnNullInputs()).isTrue();

        assertThat(compile(new Coalesce(A, B)).nullOnNullInputs()).isTrue();
        assertThat(compile(new Coalesce(A, bigint(1))).nullOnNullInputs()).isFalse();
    }

    @Test
    void testYear()
    {
        // The GPU rejects years outside cuDF's calendar, which the CPU year() accepts
        GpuExpression expression = compile(new Call(FUNCTION_RESOLUTION.resolveFunction("year", fromTypes(DATE)), ImmutableList.of(D)));
        assertThat(expression.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(expression.nullOnNullInputs()).isTrue();
    }

    @Test
    void testLogical()
    {
        GpuExpression and = compile(new Logical(Logical.Operator.AND, ImmutableList.of(C, E)));
        assertThat(and.fallibility()).isEqualTo(INFALLIBLE);
        assertThat(and.nullOnNullInputs()).isTrue();

        GpuExpression or = compile(new Logical(Logical.Operator.OR, ImmutableList.of(C, comparison(ComparisonOperator.EQUAL, divide(A, B), bigint(0)))));
        assertThat(or.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(or.nullOnNullInputs()).isTrue();

        assertThat(compile(and(C, new Constant(BOOLEAN, false))).nullOnNullInputs()).isFalse();
        assertThat(compile(and(new Constant(BOOLEAN, false), C)).nullOnNullInputs()).isFalse();
        assertThat(compile(or(C, new Constant(BOOLEAN, true))).nullOnNullInputs()).isFalse();
        assertThat(compile(or(new Constant(BOOLEAN, true), C)).nullOnNullInputs()).isFalse();
    }

    @Test
    void testIf()
    {
        GpuExpression expression = compile(ifExpression(C, divide(bigint(1), B), bigint(0)));
        assertThat(expression).isInstanceOf(GpuIf.class);
        assertThat(expression.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(expression.nullOnNullInputs()).isFalse();

        // A NULL condition selects the false branch, so the true branch is not evaluated on an all-NULL row
        Expression fallibleOnAllNulls = divide(bigint(1), new Coalesce(B, bigint(0)));
        assertThat(compile(ifExpression(C, fallibleOnAllNulls, bigint(0))).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(ifExpression(C, bigint(0), fallibleOnAllNulls)).fallibility()).isEqualTo(FALLIBLE);
        assertThat(compile(ifExpression(new Constant(BOOLEAN, true), fallibleOnAllNulls, bigint(0))).fallibility()).isEqualTo(FALLIBLE);
        assertThat(compile(ifExpression(new IsNull(B), fallibleOnAllNulls, bigint(0))).fallibility()).isEqualTo(FALLIBLE);
        assertThat(compile(ifExpression(C, A, B)).nullOnNullInputs()).isTrue();
        // A NULL condition selects the false branch
        assertThat(compile(ifExpression(C, bigint(1), B)).nullOnNullInputs()).isTrue();
        assertThat(compile(ifExpression(new Constant(BOOLEAN, true), bigint(1), B)).nullOnNullInputs()).isFalse();
    }

    @Test
    void testCase()
    {
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(C, A),
                        new WhenClause(E, divide(A, B))),
                B);
        GpuExpression compiled = compile(expression);
        assertThat(compiled).isInstanceOf(GpuCase.class);
        assertThat(compiled.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compiled.nullOnNullInputs()).isTrue();

        // A result is not evaluated on an all-NULL row when its condition is NULL there
        Expression fallibleOnAllNulls = divide(bigint(1), new Coalesce(B, bigint(0)));
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, fallibleOnAllNulls), new WhenClause(E, A)), B)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, A), new WhenClause(new IsNull(B), fallibleOnAllNulls)), B)).fallibility()).isEqualTo(FALLIBLE);

        // The default branch is selected unconditionally
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, A), new WhenClause(E, B)), fallibleOnAllNulls)).fallibility()).isEqualTo(FALLIBLE);
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, A), new WhenClause(E, B)), bigint(1))).nullOnNullInputs()).isFalse();

        // A WHEN condition that is not NULL on an all-NULL row
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, A), new WhenClause(new IsNull(B), bigint(1))), B)).nullOnNullInputs()).isFalse();
        assertThat(compile(new Case(ImmutableList.of(new WhenClause(C, A), new WhenClause(new IsNull(B), A)), B)).nullOnNullInputs()).isTrue();
    }

    @Test
    void testDateAdd()
    {
        assertThat(compile(dateAdd("day", 1, D)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        assertThat(compile(dateAdd("millisecond", 1, T)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        // A zero shift keeps every input of the nanosecond carrier in range, so there is no check to fail
        assertThat(compile(dateAdd("millisecond", 0, T)).fallibility()).isEqualTo(INFALLIBLE);
    }

    @Test
    void testConstant()
    {
        assertThat(compile(new Constant(BIGINT, null)).nullOnNullInputs()).isTrue();
        assertThat(compile(bigint(1)).nullOnNullInputs()).isFalse();
        assertThat(compile(bigint(1)).fallibility()).isEqualTo(INFALLIBLE);
    }

    @Test
    void testIsNull()
    {
        GpuExpression expression = compile(new IsNull(A));
        assertThat(expression.fallibility()).isEqualTo(INFALLIBLE);
        assertThat(expression.nullOnNullInputs()).isFalse();
    }

    @Test
    void testLet()
    {
        Symbol x = new Symbol(BIGINT, "x");
        Reference xReference = x.toSymbolReference();

        GpuExpression boundReference = compile(new Let(x, A, xReference));
        assertThat(boundReference).isInstanceOf(GpuLet.class);
        assertThat(boundReference.fallibility()).isEqualTo(INFALLIBLE);
        assertThat(boundReference.nullOnNullInputs()).isTrue();

        assertThat(compile(new Let(x, B, divide(bigint(10), xReference))).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
        // The bound column is not NULL on an all-NULL row, so the body's check fires there
        assertThat(compile(new Let(x, new Coalesce(B, bigint(0)), divide(bigint(10), xReference))).fallibility()).isEqualTo(FALLIBLE);
        assertThat(compile(new Let(x, divide(A, B), xReference)).fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
    }

    @Test
    void testPropertiesDoNotRepeatedlyTraverseSharedSubtrees()
    {
        CountingExpression leaf = new CountingExpression();
        GpuExpression expression = leaf;
        for (int i = 0; i < 30; i++) {
            expression = new GpuIntegerDivide(expression, expression, DType.INT64, "bigint");
        }
        for (int i = 0; i < 2; i++) {
            assertThat(expression.fallibility()).isEqualTo(FALLIBLE_EXCEPT_ALL_NULLS);
            assertThat(expression.nullOnNullInputs()).isTrue();
        }
        assertThat(leaf.fallibilityComputations).isEqualTo(1);
        assertThat(leaf.nullComputations).isEqualTo(1);
    }

    private static final class CountingExpression
            extends GpuExpression
    {
        private int fallibilityComputations;
        private int nullComputations;

        @Override
        public ColumnVector evaluate(int positionCount, List<ColumnVector> inputColumns)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Fallibility computeFallibility()
        {
            fallibilityComputations++;
            return INFALLIBLE;
        }

        @Override
        protected boolean computeNullOnNullInputs()
        {
            nullComputations++;
            return true;
        }

        @Override
        public boolean equals(Object obj)
        {
            return this == obj;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode(this);
        }
    }

    private static Expression and(Expression... terms)
    {
        return new Logical(Logical.Operator.AND, ImmutableList.copyOf(terms));
    }

    private static Expression or(Expression... terms)
    {
        return new Logical(Logical.Operator.OR, ImmutableList.copyOf(terms));
    }

    private static GpuExpression compile(Expression expression)
    {
        return compileExpression(expression, LAYOUT, SQL_STANDARD)
                .orElseThrow(() -> new AssertionError("Expression does not compile for GPU: " + expression))
                .expression();
    }

    private static Expression ifExpression(Expression condition, Expression trueValue, Expression falseValue)
    {
        return new Case(ImmutableList.of(new WhenClause(condition, trueValue)), falseValue);
    }

    private static Expression divide(Expression dividend, Expression divisor)
    {
        return new Call(FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT)), ImmutableList.of(dividend, divisor));
    }

    private static Expression modulo(Expression dividend, Expression divisor)
    {
        return new Call(FUNCTION_RESOLUTION.resolveOperator(OperatorType.MODULO, ImmutableList.of(BIGINT, BIGINT)), ImmutableList.of(dividend, divisor));
    }

    private static Expression dateAdd(String unit, long value, Reference dateTime)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_add", fromTypes(VARCHAR, BIGINT, dateTime.type())),
                ImmutableList.of(new Constant(VARCHAR, utf8Slice(unit)), bigint(value), dateTime));
    }

    private static Constant bigint(long value)
    {
        return new Constant(BIGINT, value);
    }
}
