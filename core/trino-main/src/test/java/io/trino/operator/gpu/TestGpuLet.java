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
package io.trino.operator.gpu;

import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.compileCpuExpression;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuLet
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testNestedLet(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // Let s1 = ref0 + ref1 in (Let s2 = s1 + ref2 in (s1 + s2))
        // The same s1 binding is addressable in both the inner value and body.
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression inner = new Let(
                s2,
                add(s1.toSymbolReference(), field(2, BIGINT)),
                add(s1.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = new Let(s1, add(field(0, BIGINT), field(1, BIGINT)), inner);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1, 2));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLetShadowsExternalSymbol(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Reference reference = field(0, BIGINT);
        // The value reads the external symbol; the body reads the binding; the sibling reads the external symbol again.
        Expression expression = add(
                new Let(Symbol.from(reference),
                        add(reference, field(1, BIGINT)),
                        add(reference, reference)),
                reference);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testLetShadowsOuterBinding(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        Symbol symbol = new Symbol(BIGINT, "s");
        Reference reference = symbol.toSymbolReference();
        // The inner value reads the outer binding, which is restored after the inner body.
        Expression inner = new Let(symbol, add(reference, field(1, BIGINT)), add(reference, reference));
        Expression expression = new Let(symbol, field(0, BIGINT), add(inner, reference));

        // The CPU compiler rejects duplicate nested Let names, so use an equivalent expression without bindings.
        Expression sum = add(field(0, BIGINT), field(1, BIGINT));
        Map<Symbol, Integer> layout = layoutFor(inputTypes);
        PageProcessor cpuProcessor = compileCpuExpression(add(add(sum, sum), field(0, BIGINT)), layout);
        CompiledExpression gpuExpression = compileExpression(expression, layout, SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertThat(gpuExpression.inputChannels().getInputChannels()).containsExactly(0, 1);
        assertGpuMatchesCpu(inputPages, inputTypes, expression, cpuProcessor, gpuExpression, false);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testSiblingLets(NullsProvider nullsProvider)
    {
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // (Let s1 = ref0 + ref1 in s1 + s1) + (Let s2 = ref0 - ref1 in s2 + s2)
        // Sibling bindings have separate scopes.
        Symbol s1 = new Symbol(BIGINT, "s1");
        Symbol s2 = new Symbol(BIGINT, "s2");
        Expression left = new Let(
                s1,
                add(field(0, BIGINT), field(1, BIGINT)),
                add(s1.toSymbolReference(), s1.toSymbolReference()));
        Expression right = new Let(
                s2,
                subtract(field(0, BIGINT), field(1, BIGINT)),
                add(s2.toSymbolReference(), s2.toSymbolReference()));
        Expression expression = add(left, right);

        assertGpuMatchesCpu(inputPages, inputTypes, expression, Set.of(0, 1));
    }

    private static Expression add(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }

    private static Expression subtract(Expression left, Expression right)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(BIGINT, BIGINT)),
                ImmutableList.of(left, right));
    }
}
