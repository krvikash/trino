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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SqlBatchFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.testing.TestingSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.operator.project.BatchProjectionUtils.compilePageFilterWithBatchFunction;
import static io.trino.spi.function.FunctionKind.BATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.toIntExact;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBatchProjection
{
    private static final Random RANDOM = new Random(5376453765L);
    private static final long CONSTANT = 64992484L;
    private static final int DOUBLE_CHANNEL = 0;
    private static final int LONG_CHANNEL_B = 1;
    private static final int STRING_CHANNEL = 2;
    private static final int LONG_CHANNEL_A = 3;
    private static final int LONG_CHANNEL_C = 4;
    private static final int LONG_CHANNEL_D = 5;
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));
    private static final TestingFunctionResolution FUNCTION_RESOLUTION;
    private static final ResolvedFunction SCALAR_ADD_BIGINT;
    private static final ResolvedFunction SCALAR_LESS_THAN_BIGINT;
    private static final ResolvedFunction SCALAR_CAST_DOUBLE;
    private static final ResolvedFunction SCALAR_CAST_STRING;
    private static final ResolvedFunction BATCH_ADD_BIGINT;
    private static final ResolvedFunction BATCH_LESS_THAN_BIGINT;

    static {
        try {
            MethodHandle addHandle = lookup().findStatic(TestBatchProjection.class, "myAdd", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
            MethodHandle lessThanHandle = lookup().findStatic(TestBatchProjection.class, "batchLessThan", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
            FunctionBundle bundle = InternalFunctionBundle.builder()
                    .function(new SqlBatchFunction(
                            batchFunction("batch_add")
                                    .description("Add two BIGINT values")
                                    .signature(signature(BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))
                                    .nullable()
                                    .build(),
                            addHandle))
                    .function(new SqlBatchFunction(
                            batchFunction("batch_less_than")
                                    .description("Compare two BIGINT values for less-than")
                                    .signature(signature(BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))
                                    .nullable()
                                    .build(),
                            lessThanHandle))
                    .build();
            FUNCTION_RESOLUTION = new TestingFunctionResolution(bundle);
            SCALAR_ADD_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
            SCALAR_CAST_DOUBLE = FUNCTION_RESOLUTION.getCoercion(DOUBLE, BIGINT);
            SCALAR_CAST_STRING = FUNCTION_RESOLUTION.getCoercion(VARCHAR, BIGINT);
            BATCH_ADD_BIGINT = FUNCTION_RESOLUTION.resolveFunction("batch_add", fromTypes(BIGINT, BIGINT));
            BATCH_LESS_THAN_BIGINT = FUNCTION_RESOLUTION.resolveFunction("batch_less_than", fromTypes(BIGINT, BIGINT));
            SCALAR_LESS_THAN_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    void testBatchFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // batch_add(#3, #1)
        RowExpression batchAdd = call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_A, BIGINT), field(LONG_CHANNEL_B, BIGINT));
        verifyProjection(inputPages, batchAdd);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    void testBatchFunctionOverNonBatchProjection(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // batch_add(#3, add(#1, #4))
        RowExpression batchAdd = call(
                BATCH_ADD_BIGINT,
                field(LONG_CHANNEL_A, BIGINT),
                call(SCALAR_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_C, BIGINT)));
        verifyProjection(inputPages, batchAdd);

        // Common input for inner and outer function
        // batch_add(#1, add(#1, #4))
        batchAdd = call(
                BATCH_ADD_BIGINT,
                field(LONG_CHANNEL_B, BIGINT),
                call(SCALAR_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_C, BIGINT)));
        verifyProjection(inputPages, batchAdd);

        // Multiple scalar projection inputs
        // batch_add(add(#1, cast(#0)), add(#1, #4))
        batchAdd = call(
                BATCH_ADD_BIGINT,
                call(SCALAR_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), call(SCALAR_CAST_DOUBLE, field(DOUBLE_CHANNEL, DOUBLE))),
                call(SCALAR_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_C, BIGINT)));
        verifyProjection(inputPages, batchAdd);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    void testNonBatchProjectionOverBatchFunction(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // add(batch_add(#3, #1), #4)
        RowExpression batchAdd = call(
                SCALAR_ADD_BIGINT,
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_A, BIGINT), field(LONG_CHANNEL_B, BIGINT)),
                field(LONG_CHANNEL_C, BIGINT));
        verifyProjection(inputPages, batchAdd);

        // add(batch_add(#3, #1), batch_add(#1, #5))
        batchAdd = call(
                SCALAR_ADD_BIGINT,
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_A, BIGINT), field(LONG_CHANNEL_B, BIGINT)),
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_D, BIGINT)));
        verifyProjection(inputPages, batchAdd);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    void testBatchFunctionMixedWithNonBatched(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        List<Page> inputPages = createInputPages(nullsProvider, dictionaryEncoded);
        // add(
        //   batch_add(#1, cast(#0)),
        //   batch_add(#1, #4))
        RowExpression batchAdd = call(
                SCALAR_ADD_BIGINT,
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), call(SCALAR_CAST_DOUBLE, field(DOUBLE_CHANNEL, DOUBLE))),
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_C, BIGINT)));
        verifyProjection(inputPages, batchAdd);

        // add(
        //   add(
        //     batch_add(cast(#2), cast(#0)),
        //     batch_add(add(#1, #5), #4)),
        //   constant)
        batchAdd = call(SCALAR_ADD_BIGINT,
                call(
                        SCALAR_ADD_BIGINT,
                        call(
                                BATCH_ADD_BIGINT,
                                call(SCALAR_CAST_STRING, field(STRING_CHANNEL, VARCHAR)),
                                call(SCALAR_CAST_DOUBLE, field(DOUBLE_CHANNEL, DOUBLE))),
                        call(
                                BATCH_ADD_BIGINT,
                                call(SCALAR_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_D, BIGINT)),
                                field(LONG_CHANNEL_C, BIGINT))),
                constant(CONSTANT, BIGINT));
        verifyProjection(inputPages, batchAdd);

        // add(
        //   batch_add(
        //     add(batch_add(#1, #1), 64992484),
        //     batch_add(#3, #5)),
        //   batch_add(#1, #4))
        batchAdd = call(
                SCALAR_ADD_BIGINT,
                call(
                        BATCH_ADD_BIGINT,
                        call(
                                SCALAR_ADD_BIGINT,
                                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_B, BIGINT)),
                                constant(CONSTANT, BIGINT)),
                        call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_A, BIGINT), field(LONG_CHANNEL_D, BIGINT))),
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_B, BIGINT), field(LONG_CHANNEL_C, BIGINT)));
        verifyProjection(inputPages, batchAdd);
    }

    @ParameterizedTest
    @MethodSource("inputProviders")
    public void testFilterWithBatchFunctions(NullsProvider nullsProvider)
    {
        List<Page> inputPages = createInputPages(nullsProvider, false);
        // batch_less_than(constant, col)
        RowExpression filter = call(
                BATCH_LESS_THAN_BIGINT,
                constant(CONSTANT, BIGINT),
                field(LONG_CHANNEL_A, BIGINT));
        verifyFilter(inputPages, filter);

        // batch_add(colA, colB) < batch_add(colC, colD)
        filter = call(
                BATCH_LESS_THAN_BIGINT,
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_A, BIGINT), field(LONG_CHANNEL_B, BIGINT)),
                call(BATCH_ADD_BIGINT, field(LONG_CHANNEL_C, BIGINT), field(LONG_CHANNEL_D, BIGINT)));
        verifyFilter(inputPages, filter);
    }

    enum NullsProvider
    {
        NO_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.empty();
            }
        },
        NO_NULLS_WITH_MAY_HAVE_NULL {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                return Optional.of(new boolean[positionCount]);
            }
        },
        ALL_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                Arrays.fill(nulls, true);
                return Optional.of(nulls);
            }
        },
        RANDOM_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    nulls[i] = RANDOM.nextBoolean();
                }
                return Optional.of(nulls);
            }
        },
        GROUPED_NULLS {
            @Override
            Optional<boolean[]> getNulls(int positionCount)
            {
                boolean[] nulls = new boolean[positionCount];
                int maxGroupSize = 23;
                int position = 0;
                while (position < positionCount) {
                    int remaining = positionCount - position;
                    int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
                    Arrays.fill(nulls, position, position + groupSize, RANDOM.nextBoolean());
                    position += groupSize;
                }
                return Optional.of(nulls);
            }
        };

        abstract Optional<boolean[]> getNulls(int positionCount);
    }

    private static Object[][] inputProviders()
    {
        return cartesianProduct(nullsProviders(), trueFalse());
    }

    private static Object[][] nullsProviders()
    {
        return Stream.of(NullsProvider.values()).collect(toDataProvider());
    }

    private static List<Page> createInputPages(NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        for (int pageCount = 0; pageCount < 20; pageCount++) {
            int positionsCount = RANDOM.nextInt(1024, 8192);
            builder.add(new Page(
                    positionsCount,
                    createDoublesBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createLongsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createStringsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createLongsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createLongsBlock(positionsCount, nullsProvider, dictionaryEncoded),
                    createLongsBlock(positionsCount, nullsProvider, dictionaryEncoded)));
        }
        return builder.build();
    }

    private static Block createLongsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            long[] dictionaryValues = new long[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = CONSTANT - 10 + i;
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new LongArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = toIntExact(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10));
            }
        }
        return new LongArrayBlock(positionsCount, isNull, values);
    }

    private static Block createDoublesBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 200;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            long[] dictionaryValues = new long[dictionarySize];
            for (int i = 0; i < nonNullDictionarySize; i++) {
                dictionaryValues[i] = doubleToLongBits(CONSTANT - 100 + i);
            }
            Optional<boolean[]> dictionaryIsNull = getDictionaryIsNull(nullsProvider, dictionarySize);
            Block dictionary = new LongArrayBlock(dictionarySize, dictionaryIsNull, dictionaryValues);
            return createDictionaryBlock(positionsCount, nullsProvider, dictionary);
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        long[] values = new long[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isEmpty() || !isNull.get()[i]) {
                values[i] = doubleToLongBits(RANDOM.nextDouble(CONSTANT - 100, CONSTANT + 100));
            }
        }
        return new LongArrayBlock(positionsCount, isNull, values);
    }

    private static Block createStringsBlock(int positionsCount, NullsProvider nullsProvider, boolean dictionaryEncoded)
    {
        if (dictionaryEncoded) {
            boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
            int nonNullDictionarySize = 20;
            int dictionarySize = nonNullDictionarySize + (containsNulls ? 1 : 0); // last element in dictionary denotes null
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, dictionarySize, dictionarySize * 10);
            for (int i = 0; i < nonNullDictionarySize; i++) {
                builder.writeEntry(Slices.utf8Slice(Long.toString(CONSTANT - 10 + i)));
            }
            if (containsNulls) {
                builder.appendNull();
            }
            return createDictionaryBlock(positionsCount, nullsProvider, builder.build());
        }

        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(Slices.utf8Slice(Long.toString(RANDOM.nextLong(CONSTANT - 10, CONSTANT + 10))));
            }
        }
        return builder.build();
    }

    private static Optional<boolean[]> getDictionaryIsNull(NullsProvider nullsProvider, int dictionarySize)
    {
        Optional<boolean[]> dictionaryIsNull = Optional.empty();
        if (nullsProvider != NullsProvider.NO_NULLS) {
            dictionaryIsNull = Optional.of(new boolean[dictionarySize]);
            if (nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL) {
                dictionaryIsNull.get()[dictionarySize - 1] = true;
            }
        }
        return dictionaryIsNull;
    }

    private static Block createDictionaryBlock(int positionsCount, NullsProvider nullsProvider, Block dictionary)
    {
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
        boolean containsNulls = nullsProvider != NullsProvider.NO_NULLS && nullsProvider != NullsProvider.NO_NULLS_WITH_MAY_HAVE_NULL;
        int dictionarySize = dictionary.getPositionCount();
        int nonNullDictionarySize = dictionarySize - (containsNulls ? 1 : 0);
        int[] ids = new int[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                ids[i] = dictionarySize - 1;
            }
            else {
                ids[i] = RANDOM.nextInt(nonNullDictionarySize);
            }
        }
        return DictionaryBlock.create(positionsCount, dictionary, ids);
    }

    private static void verifyProjection(List<Page> inputPages, RowExpression batchProjection)
    {
        RowExpression scalarProjection = rewriteBatchToScalarFunction(batchProjection);

        List<SelectedPositions> allRanges = inputPages.stream()
                .map(Page::getPositionCount)
                .map(positionCount -> SelectedPositions.positionsRange(0, positionCount))
                .collect(toImmutableList());
        verifyProjectionInternal(inputPages, allRanges, batchProjection, scalarProjection);

        List<SelectedPositions> allPositionLists = inputPages.stream()
                .map(Page::getPositionCount)
                .map(positionCount -> {
                    int[] positions = new int[positionCount];
                    for (int i = 0; i < positionCount; i++) {
                        positions[i] = i;
                    }
                    return SelectedPositions.positionsList(positions, 0, positionCount);
                })
                .collect(toImmutableList());
        verifyProjectionInternal(inputPages, allPositionLists, batchProjection, scalarProjection);

        List<SelectedPositions> randomRanges = inputPages.stream()
                .map(Page::getPositionCount)
                .map(positionCount -> {
                    int offset = RANDOM.nextInt(positionCount / 2);
                    int length = RANDOM.nextInt(positionCount - offset);
                    return SelectedPositions.positionsRange(offset, length);
                })
                .collect(toImmutableList());
        verifyProjectionInternal(inputPages, randomRanges, batchProjection, scalarProjection);

        List<SelectedPositions> randomPositionLists = inputPages.stream()
                .map(Page::getPositionCount)
                .map(positionCount -> {
                    int[] positions = new int[positionCount];
                    for (int i = 0; i < positionCount; i++) {
                        positions[i] = RANDOM.nextInt(positionCount);
                    }
                    return SelectedPositions.positionsList(positions, 0, positionCount);
                })
                .collect(toImmutableList());
        verifyProjectionInternal(inputPages, randomPositionLists, batchProjection, scalarProjection);
    }

    private static void verifyProjectionInternal(List<Page> inputPages, List<SelectedPositions> positions, RowExpression batchProjection, RowExpression scalarProjection)
    {
        List<Block> outputBlocksExpected = processProjection(inputPages, positions, scalarProjection);
        List<Block> outputBlocksActual = processProjection(inputPages, positions, batchProjection);
        assertThat(outputBlocksExpected).hasSize(outputBlocksActual.size());

        for (int i = 0; i < outputBlocksActual.size(); i++) {
            Block actual = outputBlocksActual.get(i);
            Block expected = outputBlocksExpected.get(i);
            assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
            assertBlockEquals(BIGINT, actual, expected);
        }
    }

    private static List<Block> processProjection(List<Page> inputPages, List<SelectedPositions> positions, RowExpression projection)
    {
        PageProjection pageProjection = FUNCTION_RESOLUTION.getPageFunctionCompiler().compileProjection(projection, Optional.empty()).get();
        ImmutableList.Builder<Block> outputBlocksBuilder = ImmutableList.builder();
        for (int i = 0; i < inputPages.size(); i++) {
            Page inputPage = inputPages.get(i);
            SelectedPositions selectedPositions = positions.get(i);
            Block block = pageProjection.project(
                    FULL_CONNECTOR_SESSION,
                    pageProjection.getInputChannels().getInputChannels(SourcePage.create(inputPage)),
                    selectedPositions);
            outputBlocksBuilder.add(block);
        }
        return outputBlocksBuilder.build();
    }

    static Block myAdd(ConnectorSession session, ValueBlock first, int[] firstPositions, ValueBlock second, int[] secondPositions)
    {
        int length = firstPositions.length;
        long[] result = new long[length];
        boolean[] isNull = new boolean[length];
        for (int i = 0; i < length; i++) {
            int firstPosition = firstPositions[i];
            int secondPosition = secondPositions[i];
            if (first.isNull(firstPosition) || second.isNull(secondPosition)) {
                isNull[i] = true;
            }
            else {
                result[i] = BIGINT.getLong(first, firstPosition) + BIGINT.getLong(second, secondPosition);
            }
        }
        return new LongArrayBlock(length, Optional.of(isNull), result);
    }

    static Block batchLessThan(ConnectorSession session, ValueBlock first, int[] firstPositions, ValueBlock second, int[] secondPositions)
    {
        int length = firstPositions.length;
        byte[] result = new byte[length];
        boolean[] isNull = new boolean[length];
        for (int i = 0; i < length; i++) {
            int firstPosition = firstPositions[i];
            int secondPosition = secondPositions[i];
            if (first.isNull(firstPosition) || second.isNull(secondPosition)) {
                isNull[i] = true;
            }
            else {
                result[i] = (byte) (BIGINT.getLong(first, firstPosition) < BIGINT.getLong(second, secondPosition) ? 1 : 0);
            }
        }
        return new ByteArrayBlock(length, Optional.of(isNull), result);
    }

    private static RowExpression rewriteBatchToScalarFunction(RowExpression projection)
    {
        Rewriter rewriter = new Rewriter();
        return projection.accept(rewriter, null);
    }

    private static class Rewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            ResolvedFunction function = call.resolvedFunction();
            if (call.resolvedFunction().functionKind() == BATCH) {
                if (function.equals(BATCH_ADD_BIGINT)) {
                    function = SCALAR_ADD_BIGINT;
                }
                else if (function.equals(BATCH_LESS_THAN_BIGINT)) {
                    function = SCALAR_LESS_THAN_BIGINT;
                }
                else {
                    throw new UnsupportedOperationException("Unsupported batch function: " + function);
                }
            }
            return new CallExpression(
                    function,
                    call.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            return new SpecialForm(
                    specialForm.form(),
                    specialForm.type(),
                    specialForm.arguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()),
                    specialForm.functionDependencies());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(
                    lambda.arguments(),
                    lambda.body().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }
    }

    private static void verifyFilter(List<Page> inputPages, RowExpression filter)
    {
        PageFilter pageFilter = compilePageFilterWithBatchFunction(filter, Optional.empty(), FUNCTION_RESOLUTION.getPageFunctionCompiler())
                .orElseThrow(() -> new IllegalArgumentException("Expected filter to contain batch function"))
                .get();
        List<SelectedPositions> expectedPositions = processFilter(inputPages, pageFilter);
        RowExpression scalarFilter = rewriteBatchToScalarFunction(filter);
        pageFilter = FUNCTION_RESOLUTION.getPageFunctionCompiler().compileFilter(scalarFilter, Optional.empty()).get();
        List<SelectedPositions> actualPositions = processFilter(inputPages, pageFilter);
        assertThat(expectedPositions).hasSize(actualPositions.size());

        for (int pageCount = 0; pageCount < actualPositions.size(); pageCount++) {
            assertThat(toSet(actualPositions.get(pageCount))).isEqualTo(toSet(expectedPositions.get(pageCount)));
        }
    }

    private static List<SelectedPositions> processFilter(List<Page> inputPages, PageFilter filter)
    {
        ImmutableList.Builder<SelectedPositions> positionsBuilder = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            positionsBuilder.add(filter.filter(
                    FULL_CONNECTOR_SESSION,
                    filter.getInputChannels().getInputChannels(SourcePage.create(inputPage))));
        }
        return positionsBuilder.build();
    }

    private static FunctionMetadata.Builder batchFunction(String name)
    {
        return FunctionMetadata.batchBuilder(name).functionId(new FunctionId(name));
    }

    private static Signature signature(TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return Signature.builder()
                .returnType(returnType)
                .argumentTypes(List.of(argumentTypes))
                .build();
    }

    private static Set<Integer> toSet(SelectedPositions positions)
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        if (positions.isList()) {
            for (int index = positions.getOffset(); index < positions.getOffset() + positions.size(); index++) {
                builder.add(positions.getPositions()[index]);
            }
        }
        else {
            for (int position = positions.getOffset(); position < positions.getOffset() + positions.size(); position++) {
                builder.add(position);
            }
        }
        return builder.build();
    }
}
