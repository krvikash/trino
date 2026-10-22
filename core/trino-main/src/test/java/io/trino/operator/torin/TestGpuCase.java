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
package io.trino.operator.torin;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.createBigintBlock;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuCase
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testCaseEmptyInput()
    {
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                0,
                createBigintBlock(0, NullsProvider.NO_NULLS, -100, 100)));

        int channelA = 0;
        Expression aLtZero = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 0L));
        Expression aLtFifty = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 50L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aLtZero, new Constant(BIGINT, -1L)),
                        new WhenClause(aLtFifty, new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 1L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseNoMatch()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a = 999 THEN 1 WHEN a = 998 THEN 2 ELSE 42 END
        Expression aEq999 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 999L));
        Expression aEq998 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 998L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aEq999, new Constant(BIGINT, 1L)),
                        new WhenClause(aEq998, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 42L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseNoMatchNoElse()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a = 999 THEN 1 WHEN a = 998 THEN 2 END → all rows get null
        Expression aEq999 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 999L));
        Expression aEq998 = comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, 998L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aEq999, new Constant(BIGINT, 1L)),
                        new WhenClause(aEq998, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseNullCondition()
    {
        // CASE WHEN null THEN 1 WHEN null THEN 2 ELSE 3 END
        // Both conditions are literal NULLs, treated as FALSE → all rows hit default.
        int positionsCount = 64;
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, null), new Constant(BIGINT, 1L)),
                        new WhenClause(new Constant(BOOLEAN, null), new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(TEST_SESSION, inputPages, inputTypes, expression, Set.of());
    }

    @Test
    void testCaseNullConditionGuardsFallibleResult()
    {
        // CASE WHEN b > 0 THEN a / coalesce(b, 0) WHEN true THEN 7 ELSE -3 END
        // When b is NULL, the condition (b > 0) evaluates to NULL → treated as FALSE.
        // The result a / coalesce(b, 0) must NOT be evaluated on those rows — coalesce
        // would resurrect 0 from the NULL and trigger DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, 100L);
            if (i % 2 == 0) {
                bBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(bBuilder, i);
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression coalesceB = new Coalesce(ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 0L)));
        Expression aDivCoalesceB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), coalesceB));
        Expression bPositive = comparison(ComparisonOperator.GREATER_THAN, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bPositive, aDivCoalesceB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 7L))),
                new Constant(BIGINT, -3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseNoInputColumns()
    {
        // CASE WHEN true THEN 1 WHEN false THEN 2 ELSE 3 END
        int positionsCount = 64;
        List<Type> inputTypes = List.of(BIGINT);
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 1L)),
                        new WhenClause(new Constant(BOOLEAN, false), new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(TEST_SESSION, inputPages, inputTypes, expression, Set.of());
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testCaseManyBranches(NullsProvider nullsProvider)
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, 0, 15)));

        // CASE WHEN a = 0 THEN 0 WHEN a = 1 THEN 1 ... WHEN a = 9 THEN 9 ELSE -1 END
        ImmutableList.Builder<WhenClause> whenClauses = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            whenClauses.add(new WhenClause(
                    comparison(ComparisonOperator.EQUAL, field(channelA, BIGINT), new Constant(BIGINT, (long) i)),
                    new Constant(BIGINT, (long) i)));
        }
        Expression expression = new Case(whenClauses.build(), new Constant(BIGINT, -1L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseLazyCondition()
    {
        // CASE WHEN b = 0 THEN 1 WHEN (a / b) > 5 THEN 2 ELSE 3 END
        // Rows where b = 0 match the first branch. The second branch's condition (a / b)
        // is never evaluated on those rows, avoiding DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bEqZero = comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression aDivBGt5 = comparison(ComparisonOperator.GREATER_THAN, aDivB, new Constant(BIGINT, 5L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bEqZero, new Constant(BIGINT, 1L)),
                        new WhenClause(aDivBGt5, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseLazyResult()
    {
        // CASE WHEN b != 0 THEN a / b WHEN true THEN 0 END
        // a / b is only evaluated on rows where b != 0.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            BIGINT.writeLong(bBuilder, switch (i % 3) {
                case 0 -> 0L;
                case 1 -> random.nextLong(1, 100);
                default -> -random.nextLong(1, 100);
            });
        }
        List<Page> inputPages = List.of(new Page(positionsCount, aBuilder.build(), bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(bNotZero, aDivB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseFailingCondition()
    {
        // CASE WHEN false THEN 1 WHEN (a / b) > 5 THEN 2 ELSE 3 END
        // First branch doesn't match, so the second condition (a / b) IS evaluated with b = 0.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(aBuilder, 10L);
        BIGINT.writeLong(bBuilder, 0L);
        List<Page> inputPages = List.of(new Page(1, aBuilder.build(), bBuilder.build()));

        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression aDivBGt5 = comparison(ComparisonOperator.GREATER_THAN, aDivB, new Constant(BIGINT, 5L));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, false), new Constant(BIGINT, 1L)),
                        new WhenClause(aDivBGt5, new Constant(BIGINT, 2L))),
                new Constant(BIGINT, 3L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseFailingResult()
    {
        // CASE WHEN true THEN a / b WHEN true THEN 0 END
        // First branch matches and evaluates a / b with b = 0 → DIVISION_BY_ZERO.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(aBuilder, 10L);
        BIGINT.writeLong(bBuilder, 0L);
        List<Page> inputPages = List.of(new Page(1, aBuilder.build(), bBuilder.build()));

        Expression aDivB = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), field(channelB, BIGINT)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, true), aDivB),
                        new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, null));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testCaseComplexResultExpressions()
    {
        int channelA = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, NullsProvider.NO_NULLS, -100, 100)));

        // CASE WHEN a > 0 THEN a + 1 WHEN a < 0 THEN a * 2 ELSE a END
        Expression aRef = field(channelA, BIGINT);
        Expression aGtZero = comparison(ComparisonOperator.GREATER_THAN, aRef, new Constant(BIGINT, 0L));
        Expression aLtZero = comparison(ComparisonOperator.LESS_THAN, aRef, new Constant(BIGINT, 0L));
        Expression aPlus1 = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(aRef, new Constant(BIGINT, 1L)));
        Expression aTimes2 = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.MULTIPLY, List.of(BIGINT, BIGINT)),
                ImmutableList.of(aRef, new Constant(BIGINT, 2L)));
        Expression expression = new Case(
                ImmutableList.of(
                        new WhenClause(aGtZero, aPlus1),
                        new WhenClause(aLtZero, aTimes2)),
                aRef);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }
}
