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
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
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
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.divisorPages;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.TestingIr.comparison;
import static java.lang.Float.floatToRawIntBits;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuIf
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIf(NullsProvider nullsProvider)
    {
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBigintBlock(positionsCount, nullsProvider, -100, 100),
                createBigintBlock(positionsCount, nullsProvider, -100, 100)));

        // IF(a < 50, a, b)
        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(channelA, BIGINT), new Constant(BIGINT, 50L));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(condition, field(channelA, BIGINT))),
                field(channelB, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @Test
    void testIfLazyEvaluation()
    {
        // IF(b != 0, a / b, a) must not throw division-by-zero on rows where b = 0,
        // because the CPU short-circuits and never evaluates the divide for those rows.
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(BIGINT, BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder aBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            BIGINT.writeLong(aBuilder, random.nextLong(-1000, 1000));
            // Mix zero, negative, and positive divisors so both branches are exercised.
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
                ImmutableList.of(new WhenClause(bNotZero, aDivB)),
                field(channelA, BIGINT));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfLazyEvaluationDefeatsInputMasking(NullsProvider nullsProvider)
    {
        // IF(b != 0, 10 / coalesce(b, 0), 42): the true branch reads b, but coalesce
        // resurrects a non-null 0 from a NULL b. CPU short-circuits and never invokes the
        // divide for rows with b = 0 or b IS NULL, so the result is 42 there. The GPU must
        // not evaluate the true branch on those rows either — input-NULL masking would
        // be defeated by coalesce and trigger DIVISION_BY_ZERO.
        int channelB = 0;
        List<Type> inputTypes = List.of(BIGINT);
        int positionsCount = 64;
        Random random = new Random(42);
        BlockBuilder bBuilder = BIGINT.createBlockBuilder(null, positionsCount);
        long[] isValid = nullsProvider.getValidityWords(positionsCount).orElse(Bitmap.allocateWords(positionsCount, true));
        for (int i = 0; i < positionsCount; i++) {
            if (!Bitmap.isSet(isValid, 0, i)) {
                bBuilder.appendNull();
            }
            else {
                // Mix zero and non-zero divisors so both branches are exercised.
                BIGINT.writeLong(bBuilder, i % 3 == 0 ? 0L : random.nextLong(1, 100));
            }
        }
        List<Page> inputPages = List.of(new Page(positionsCount, bBuilder.build()));

        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression coalesceB = new Coalesce(ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 0L)));
        Expression divide = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(new Constant(BIGINT, 10L), coalesceB));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, divide)),
                new Constant(BIGINT, 42L));

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfInfallibleBranches(NullsProvider nullsProvider)
    {
        // IF(a < 50, a + 1000000, b - 1000000) over REAL, whose arithmetic raises nothing
        int channelA = 0;
        int channelB = 1;
        List<Type> inputTypes = List.of(REAL, REAL);
        int positionsCount = 64;
        List<Page> inputPages = List.of(new Page(
                positionsCount,
                createBlock(REAL, positionsCount, nullsProvider),
                createBlock(REAL, positionsCount, nullsProvider)));

        Expression condition = comparison(ComparisonOperator.LESS_THAN, field(channelA, REAL), new Constant(REAL, (long) floatToRawIntBits(50.0f)));
        Expression add = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(REAL, REAL)),
                ImmutableList.of(field(channelA, REAL), new Constant(REAL, (long) floatToRawIntBits(1000000.0f))));
        Expression subtract = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.SUBTRACT, List.of(REAL, REAL)),
                ImmutableList.of(field(channelB, REAL), new Constant(REAL, (long) floatToRawIntBits(1000000.0f))));
        Expression expression = new Case(ImmutableList.of(new WhenClause(condition, add)), subtract);

        assertGpuMatchesCpu(inputPages, inputTypes, expression);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfTrueBranchFallibleExceptAllNulls(NullsProvider nullsProvider)
    {
        // IF(b <> 0, a / b, 42): a NULL b makes the divide yield NULL rather than raise
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), ifDivide(true, field(1, BIGINT)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfFalseBranchFallibleExceptAllNulls(NullsProvider nullsProvider)
    {
        // IF(b = 0, 42, a / b)
        assertGpuMatchesCpu(divisorPages(nullsProvider), List.of(BIGINT, BIGINT), ifDivide(false, field(1, BIGINT)));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfTrueBranchFallible(NullsProvider nullsProvider)
    {
        // IF(b <> 0, 10 / coalesce(b, 7), 42): a constant dividend leaves nothing to mask, and coalesce
        // revives a non-NULL divisor from a NULL b, so the divide can raise on any row. It stays within
        // range on the rows the true branch covers, where b is neither NULL nor zero.
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), ifDivideConstant(true, field(1, BIGINT)), Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfFalseBranchFallible(NullsProvider nullsProvider)
    {
        // IF(b = 0, 42, 10 / coalesce(b, 7)): the false branch covers a NULL b, where coalesce yields 7
        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), ifDivideConstant(false, field(1, BIGINT)), Set.of(1));
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testIfBothBranchesFallible(NullsProvider nullsProvider)
    {
        // IF(b <> 0, 10 / coalesce(b, 7), 10 / coalesce(b + 1, 7)): each divide can raise on any row, and
        // each divisor stays in range on the rows its own branch covers — the true branch sees only a
        // non-zero b, the false branch only a zero or NULL one.
        int channelB = 1;
        Expression bNotZero = new Call(
                FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                ImmutableList.of(comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L))));
        Expression bPlusOne = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelB, BIGINT), new Constant(BIGINT, 1L)));
        Expression expression = new Case(
                ImmutableList.of(new WhenClause(bNotZero, divideConstantByCoalesce(field(channelB, BIGINT)))),
                divideConstantByCoalesce(bPlusOne));

        // TODO divisorPages generates two columns, but the test uses only one
        assertGpuMatchesCpu(TEST_SESSION, divisorPages(nullsProvider), List.of(BIGINT, BIGINT), expression, Set.of(channelB));
    }

    /// `10 / coalesce(divisor, 7)`, which can raise on any row: the check the divide runs is masked by
    /// the dividend being non-NULL, and a constant dividend never is.
    private static Expression divideConstantByCoalesce(Expression divisor)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(
                        new Constant(BIGINT, 10L),
                        new Coalesce(ImmutableList.of(divisor, new Constant(BIGINT, 7L)))));
    }

    /// `IF(b <> 0, 10 / coalesce(divisor, 7), 42)` when `divideInTrueBranch`,
    /// otherwise `IF(b = 0, 42, 10 / coalesce(divisor, 7))`.
    private static Expression ifDivideConstant(boolean divideInTrueBranch, Expression divisor)
    {
        int channelB = 1;
        Expression bIsZero = comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression divide = divideConstantByCoalesce(divisor);
        Expression fallback = new Constant(BIGINT, 42L);
        if (divideInTrueBranch) {
            Expression bNotZero = new Call(
                    FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                    ImmutableList.of(bIsZero));
            return new Case(ImmutableList.of(new WhenClause(bNotZero, divide)), fallback);
        }
        return new Case(ImmutableList.of(new WhenClause(bIsZero, fallback)), divide);
    }

    /// `IF(b <> 0, a / divisor, 42)` when `divideInTrueBranch`, otherwise `IF(b = 0, 42, a / divisor)`.
    private static Expression ifDivide(boolean divideInTrueBranch, Expression divisor)
    {
        int channelA = 0;
        int channelB = 1;
        Expression bIsZero = comparison(ComparisonOperator.EQUAL, field(channelB, BIGINT), new Constant(BIGINT, 0L));
        Expression divide = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.DIVIDE, List.of(BIGINT, BIGINT)),
                ImmutableList.of(field(channelA, BIGINT), divisor));
        Expression fallback = new Constant(BIGINT, 42L);
        if (divideInTrueBranch) {
            Expression bNotZero = new Call(
                    FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                    ImmutableList.of(bIsZero));
            return new Case(ImmutableList.of(new WhenClause(bNotZero, divide)), fallback);
        }
        return new Case(ImmutableList.of(new WhenClause(bIsZero, fallback)), divide);
    }
}
