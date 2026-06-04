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
import ai.rapids.cudf.Table;
import ai.rapids.cudf.ast.AstExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.GpuProject;
import io.trino.operator.gpu.join.CudfAstExpression;
import io.trino.operator.project.InputChannels;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorType;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.executeWithCpu;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
class TestGpuExpressionAstCompiler
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testReference()
    {
        Reference reference = field(0, BIGINT);
        assertCompilesAndExecutes(reference, List.of(BIGINT));
    }

    @Test
    void testBooleanComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.EQUAL, field(0, BOOLEAN), new Constant(BOOLEAN, true));
        assertCompilesAndExecutes(expression, List.of(BOOLEAN));
    }

    @Test
    void testTinyintComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.EQUAL, field(0, TINYINT), new Constant(TINYINT, 7L));
        assertCompilesAndExecutes(expression, List.of(TINYINT));
    }

    @Test
    void testSmallintComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.LESS_THAN, field(0, SMALLINT), new Constant(SMALLINT, 100L));
        assertCompilesAndExecutes(expression, List.of(SMALLINT));
    }

    @Test
    void testIntegerComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.GREATER_THAN_OR_EQUAL, field(0, INTEGER), new Constant(INTEGER, 10L));
        assertCompilesAndExecutes(expression, List.of(INTEGER));
    }

    @Test
    void testBigintComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.NOT_EQUAL, field(0, BIGINT), new Constant(BIGINT, 1234567890123L));
        assertCompilesAndExecutes(expression, List.of(BIGINT));
    }

    @Test
    void testRealComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.LESS_THAN_OR_EQUAL, field(0, REAL), new Constant(REAL, (long) floatToIntBits(1.5f)));
        assertCompilesAndExecutes(expression, List.of(REAL));
    }

    @Test
    void testDoubleComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.GREATER_THAN, field(0, DOUBLE), new Constant(DOUBLE, 0.5));
        assertCompilesAndExecutes(expression, List.of(DOUBLE));
    }

    @Test
    void testDateComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.LESS_THAN, field(0, DATE), new Constant(DATE, 18000L));
        assertCompilesAndExecutes(expression, List.of(DATE));
    }

    @Test
    void testVarcharComparison()
    {
        Expression expression = new Comparison(Comparison.Operator.EQUAL, field(0, VARCHAR), new Constant(VARCHAR, Slices.utf8Slice("foo")));
        assertCompilesAndExecutes(expression, List.of(VARCHAR));
    }

    @Test
    void testAllComparisonOperators()
    {
        for (Comparison.Operator operator : Comparison.Operator.values()) {
            if (operator == Comparison.Operator.IDENTICAL) {
                continue;
            }
            Expression expression = new Comparison(operator, field(0, BIGINT), new Constant(BIGINT, 0L));
            assertCompilesAndExecutes(expression, List.of(BIGINT));
        }
    }

    @Test
    void testIdenticalNotCompiled()
    {
        assertDoesNotCompile(new Comparison(Comparison.Operator.IDENTICAL, field(0, BIGINT), field(1, BIGINT)));
    }

    @Test
    void testComparisonOfUnsupportedTypesNotCompiled()
    {
        DecimalType decimalType = createDecimalType(10, 2);
        assertDoesNotCompile(new Comparison(
                Comparison.Operator.EQUAL,
                new Constant(decimalType, 100L),
                new Constant(decimalType, 200L)));
    }

    @Test
    void testUnsupportedConstantTypeNotCompiled()
    {
        assertDoesNotCompile(new Constant(createDecimalType(10, 2), 12345L));
    }

    @Test
    void testLogicalAnd()
    {
        Expression a = new Comparison(Comparison.Operator.GREATER_THAN, field(0, BIGINT), new Constant(BIGINT, 0L));
        Expression b = new Comparison(Comparison.Operator.LESS_THAN, field(0, BIGINT), new Constant(BIGINT, 10L));
        assertCompilesAndExecutes(new Logical(Logical.Operator.AND, List.of(a, b)), List.of(BIGINT));
    }

    @Test
    void testLogicalOr()
    {
        Expression a = new Comparison(Comparison.Operator.LESS_THAN, field(0, BIGINT), new Constant(BIGINT, 0L));
        Expression b = new Comparison(Comparison.Operator.GREATER_THAN, field(0, BIGINT), new Constant(BIGINT, 10L));
        assertCompilesAndExecutes(new Logical(Logical.Operator.OR, List.of(a, b)), List.of(BIGINT));
    }

    @Test
    void testLogicalAndManyTerms()
    {
        // Exercises the left-deep tree the compiler builds for AND of more than 2 terms.
        Expression a = new IsNull(field(0, BIGINT));
        Expression b = new IsNull(field(1, BIGINT));
        Expression c = new IsNull(field(2, BIGINT));
        Expression d = new IsNull(field(3, BIGINT));
        assertCompilesAndExecutes(
                new Logical(Logical.Operator.AND, List.of(a, b, c, d)),
                List.of(BIGINT, BIGINT, BIGINT, BIGINT));
    }

    @Test
    void testLogicalAndWithUnsupportedTermNotCompiled()
    {
        DecimalType decimalType = createDecimalType(10, 2);
        Expression supported = new IsNull(field(0, BIGINT));
        Expression unsupported = new Comparison(
                Comparison.Operator.EQUAL,
                new Constant(decimalType, 1L),
                new Constant(decimalType, 2L));
        assertDoesNotCompile(new Logical(Logical.Operator.AND, List.of(supported, unsupported)));
        assertDoesNotCompile(new Logical(Logical.Operator.AND, List.of(unsupported, supported)));
    }

    @Test
    void testIn()
    {
        // empty list
        assertDoesNotCompile(new In(field(0, BIGINT), List.of()));

        // single list element
        assertCompilesAndExecutes(
                new In(field(0, BIGINT), List.of(new Constant(BIGINT, 1L))),
                List.of(BIGINT));

        // multiple elements
        assertCompilesAndExecutes(
                new In(field(0, BIGINT),
                        List.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 7L), new Constant(BIGINT, 42L))),
                List.of(BIGINT));

        // too many elements
        assertDoesNotCompile(
                new In(field(0, BIGINT),
                        IntStream.rangeClosed(1, 21)
                                .mapToObj(i -> (Expression) new Constant(BIGINT, (long) i))
                                .toList()));
    }

    @Test
    void testBetween()
    {
        assertCompilesAndExecutes(
                new Between(
                        field(0, BigintType.BIGINT),
                        new Constant(BigintType.BIGINT, 0L),
                        new Constant(BigintType.BIGINT, 10L)),
                List.of(BIGINT));
    }

    @Test
    void testIsNull()
    {
        assertCompilesAndExecutes(new IsNull(field(0, BIGINT)), List.of(BIGINT));
    }

    @Test
    void testNotCall()
    {
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(new IsNull(field(0, BIGINT))));
        assertCompilesAndExecutes(expression, List.of(BIGINT));
    }

    @Test
    void testNonBuiltinCallNotCompiled()
    {
        Expression expression = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(0, BIGINT), field(1, BIGINT)));
        assertDoesNotCompile(expression);
    }

    private void assertCompilesAndExecutes(Expression expression, List<Type> inputTypes)
    {
        Map<Symbol, Integer> layout = layoutFor(inputTypes);
        CudfAstExpression compiled = compile(expression)
                .orElseThrow(() -> new AssertionError("GPU AST compile failed: " + expression));
        AstExpression cudfAst = compiled.toCudfAst(layout, Map.of());

        List<Integer> channels = IntStream.range(0, inputTypes.size()).boxed().toList();
        CompiledExpression gpuExpression = new CompiledExpression(
                (_, columns) -> evaluateAst(cudfAst, columns),
                new InputChannels(channels));

        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(positionsCount, inputTypes.stream()
                .map(type -> createBlock(type, positionsCount, NullsProvider.RANDOM_NULLS))
                .toArray(Block[]::new)));

        List<Page> cpuResults = executeWithCpu(inputPages, List.of(expression), layout);
        List<Page> gpuResults = executeGpuOperation(
                inputPages,
                inputTypes,
                List.of(expression.type()),
                (context, copyToDevice) -> new GpuProject(context, copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))));

        assertSameDataInOrder(gpuResults, cpuResults, List.of(expression.type()));
    }

    private static void assertDoesNotCompile(Expression expression)
    {
        assertThat(compile(expression))
                .as("compile(%s)", expression)
                .isEmpty();
    }

    private static @Move ColumnVector evaluateAst(AstExpression cudfAst, @Borrow List<ColumnVector> inputs)
    {
        try (Table table = new Table(inputs.toArray(new ColumnVector[0]));
                var compiled = cudfAst.compile()) {
            return compiled.computeColumn(table);
        }
    }

    private static Reference field(int channel, Type type)
    {
        return new Reference(type, "ref" + channel);
    }

    private static Map<Symbol, Integer> layoutFor(List<Type> inputTypes)
    {
        ImmutableMap.Builder<Symbol, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < inputTypes.size(); i++) {
            builder.put(new Symbol(inputTypes.get(i), "ref" + i), i);
        }
        return builder.buildOrThrow();
    }

    private static Optional<CudfAstExpression> compile(Expression expression)
    {
        return GpuExpressionAstCompiler.compile(expression);
    }
}
