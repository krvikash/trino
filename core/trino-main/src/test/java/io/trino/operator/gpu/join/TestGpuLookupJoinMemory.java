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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ast.AstExpression;
import ai.rapids.cudf.ast.BinaryOperation;
import ai.rapids.cudf.ast.BinaryOperator;
import ai.rapids.cudf.ast.ColumnReference;
import ai.rapids.cudf.ast.TableReference;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.join.GpuLookupJoin.JoinType;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createBigintBlock;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_BYTES;
import static io.trino.operator.gpu.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_ROWS;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.FULL;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.LEFT;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.RIGHT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static java.lang.Math.ceilDiv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuLookupJoinMemory
{
    private static final int BUILD_ROWS = 100_000;
    private static final int PROBE_ROWS_PER_PAGE = 200_000;
    private static final int CHUNK_ROWS = 100_000;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testEmptyProbe(JoinType joinType)
    {
        testProbe(joinType, List.of(BIGINT), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 0);
    }

    @Test
    void testEmptyBuildLeftJoin()
    {
        testProbe(LEFT, List.of(BIGINT), new int[] {0}, new int[] {0}, new int[] {}, ImmutableList.of(BIGINT), 0, 1);
    }

    @Test
    void testEmptyBuildRightJoin()
    {
        // The drain has no build rows to emit, so the output is empty
        testProbe(RIGHT, List.of(BIGINT), new int[] {0}, new int[] {0}, new int[] {}, ImmutableList.of(BIGINT), 0, 1);
    }

    @Test
    void testEmptyBuildFullJoin()
    {
        // The probe rows are emitted with NULL build columns, and the drain has no build rows to emit
        testProbe(FULL, List.of(BIGINT), new int[] {0}, new int[] {0}, new int[] {}, ImmutableList.of(BIGINT), 0, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testBigintKey(JoinType joinType)
    {
        testProbe(joinType, List.of(BIGINT), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testVarcharKey(JoinType joinType)
    {
        testProbe(joinType, List.of(VARCHAR), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testCompositeKey(JoinType joinType)
    {
        testProbe(
                joinType,
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, createDecimalType(18, 3)),
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new int[] {3, 4},
                List.of(VARCHAR, createDecimalType(18, 3)),
                1,
                1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testHighFanOutChunked(JoinType joinType)
    {
        // Build keys repeat about 10 times and half of the probe rows match one
        List<Type> types = List.of(BIGINT, BIGINT);
        for (NullsProvider nullsProvider : List.of(NO_NULLS, RANDOM_NULLS)) {
            for (Optional<AstExpression> filter : List.of(
                    Optional.<AstExpression>empty(),
                    // probe.value < build.value
                    Optional.<AstExpression>of(new BinaryOperation(
                            BinaryOperator.LESS,
                            new ColumnReference(1, TableReference.LEFT),
                            new ColumnReference(1, TableReference.RIGHT))))) {
                List<Integer> probeOutputPageRows = testProbe(
                        joinType,
                        types,
                        new int[] {0},
                        new int[] {1},
                        new int[] {0, 1},
                        List.of(BIGINT),
                        filter,
                        List.of(keyValuePage(nullsProvider, BUILD_ROWS, BUILD_ROWS / 10)).iterator(),
                        List.of(keyValuePage(nullsProvider, PROBE_ROWS_PER_PAGE, BUILD_ROWS / 5)).iterator(),
                        filter.isPresent(),
                        CHUNK_ROWS);
                int outputRows = probeOutputPageRows.stream().mapToInt(Integer::intValue).sum();
                assertThat(probeOutputPageRows)
                        .hasSize(ceilDiv(outputRows, CHUNK_ROWS))
                        .allSatisfy(rows -> assertThat(rows).isLessThanOrEqualTo(CHUNK_ROWS));
                if (filter.isEmpty() && nullsProvider == NO_NULLS) {
                    assertThat(outputRows).isGreaterThan(5 * CHUNK_ROWS);
                }
            }
        }
    }

    /// One probe row far wider than the rest, all on a single key, so the output needs several chunks only
    /// once their row count follows the widest row rather than the page average.
    @Test
    void testChunkRowsBoundedByWidestRow()
    {
        int probeRows = 250_000;
        int buildRows = 10;
        int wideRowChars = 4_000;
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<Integer> probeOutputPageRows = testProbe(
                JoinType.INNER,
                types,
                new int[] {0},
                new int[] {},
                new int[] {0, 1},
                ImmutableList.of(),
                Optional.empty(),
                List.of(new Page(zerosBlock(buildRows), wideRowsFirst(buildRows, 0, 0))).iterator(),
                List.of(new Page(zerosBlock(probeRows), wideRowsFirst(probeRows, 1, wideRowChars))).iterator(),
                false,
                MAX_OUTPUT_CHUNK_ROWS);
        assertThat(probeOutputPageRows.stream().mapToInt(Integer::intValue).sum()).isEqualTo(probeRows * buildRows);
        assertThat(probeOutputPageRows)
                .hasSizeGreaterThan(1)
                .allSatisfy(rows -> assertThat((long) rows).isLessThanOrEqualTo(MAX_OUTPUT_CHUNK_BYTES / wideRowChars));
    }

    private static Block zerosBlock(int rows)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            BIGINT.writeLong(builder, 0);
        }
        return builder.build();
    }

    /// `rows` varchar rows of which the leading `wideRows` hold `wideRowChars` characters and the
    /// rest hold one.
    private static Block wideRowsFirst(int rows, int wideRows, int wideRowChars)
    {
        Slice wide = Slices.utf8Slice("x".repeat(wideRowChars));
        Slice narrow = Slices.utf8Slice("y");
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, rows, wideRows * wideRowChars + rows);
        for (int row = 0; row < wideRows; row++) {
            builder.writeEntry(wide);
        }
        for (int row = wideRows; row < rows; row++) {
            builder.writeEntry(narrow);
        }
        return builder.build();
    }

    private static Page keyValuePage(NullsProvider nullsProvider, int rows, int distinctKeys)
    {
        return new Page(
                createBigintBlock(rows, nullsProvider, 0, distinctKeys),
                createBigintBlock(rows, nullsProvider, 0, 1_000_000));
    }

    private void testProbe(
            JoinType joinType,
            List<Type> types,
            int[] keyChannels,
            int[] buildOutputChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            int buildPageCount,
            int probePageCount)
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            // non-filtered
            testProbe(
                    joinType,
                    types,
                    nullsProvider,
                    keyChannels,
                    buildOutputChannels,
                    probeOutputChannels,
                    buildOutputTypes,
                    Optional.empty(),
                    buildPageCount,
                    probePageCount,
                    false);
            // filtered
            testProbe(
                    joinType,
                    types,
                    nullsProvider,
                    keyChannels,
                    buildOutputChannels,
                    probeOutputChannels,
                    buildOutputTypes,
                    Optional.of(new BinaryOperation(
                            BinaryOperator.NOT_EQUAL,
                            new ColumnReference(0, TableReference.LEFT),
                            new ColumnReference(0, TableReference.RIGHT))),
                    buildPageCount,
                    probePageCount,
                    true /* Larger margin because for filtered joins the output size (gather maps + gathered tables) is not estimated upfront. */);
        }
    }

    private void testProbe(
            JoinType joinType,
            List<Type> types,
            NullsProvider nullsProvider,
            int[] keyChannels,
            int[] buildOutputChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            Optional<AstExpression> filter,
            int buildPageCount,
            int probePageCount,
            boolean largerMargin)
    {
        testProbe(
                joinType,
                types,
                keyChannels,
                buildOutputChannels,
                probeOutputChannels,
                buildOutputTypes,
                filter,
                createPages(types, nullsProvider, buildPageCount, BUILD_ROWS, false),
                createPages(types, nullsProvider, probePageCount, PROBE_ROWS_PER_PAGE, true),
                largerMargin,
                MAX_OUTPUT_CHUNK_ROWS);
    }

    /// Returns the row counts of the pages the probe operator emits.
    private List<Integer> testProbe(
            JoinType joinType,
            List<Type> types,
            int[] keyChannels,
            int[] buildOutputChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            Optional<AstExpression> filter,
            Iterator<Page> buildPages,
            Iterator<Page> probePages,
            boolean largerMargin,
            int maxOutputChunkRows)
    {
        boolean filteredJoin = filter.isPresent();
        List<Integer> probeOutputPageRows;
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        long marginBytes = (largerMargin ? 9L : 1L) * 1024 * 1024;
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, marginBytes)) {
            GpuJoinBridgeManager manager = new GpuJoinBridgeManager(joinType.isBuildOuter());
            GpuJoinBuild.Factory buildFactory = new GpuJoinBuild.Factory(manager, keyChannels, buildOutputChannels, filter, Optional.empty());

            try (var buildSource = new SettableGpuOperation();
                    GpuOperation build = buildFactory.create(context, buildSource)) {
                // Drive build to published — exempt from validation since we're testing probe
                memoryValidation.withoutValidation(() -> {
                    boolean buildBlocked = false;
                    while (!buildBlocked) {
                        switch (build.execute()) {
                            case Yielded() -> {
                                if (!buildSource.hasPending()) {
                                    if (buildPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(buildPages.next()), types));
                                        buildSource.setPending(context.taskMemoryContext().allocate("build", next.retainedMemory()), next);
                                    }
                                    else {
                                        buildSource.noMoreInput();
                                    }
                                }
                            }
                            case Blocked _ -> buildBlocked = true;
                            case Data _ -> throw new IllegalStateException("Build should not emit data");
                            case Finished() -> throw new IllegalStateException("Unexpected finish");
                        }
                    }
                });

                // Drive probe with memory validation active
                List<Type> probeOutputTypes = IntStream.of(probeOutputChannels).mapToObj(types::get).collect(toImmutableList());
                GpuLookupJoin.Factory probeFactory = new GpuLookupJoin.Factory(manager, keyChannels, probeOutputChannels, joinType, buildOutputTypes, maxOutputChunkRows, filteredJoin);
                if (joinType.isBuildOuter()) {
                    try (var probeSource = new SettableGpuOperation();
                            GpuOperation probe = probeFactory.create(context, probeSource)) {
                        probeFactory.noMoreOperators();
                        probeOutputPageRows = drainJoinOutput(probe, probeSource, probePages, types, context, memoryValidation);
                    }
                    // Separate outer driver, run after the probe so the barrier has fired and the drain never blocks
                    GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeOutputTypes);
                    try (var drainSource = new SettableGpuOperation();
                            GpuOperation drain = drainFactory.create(context, drainSource)) {
                        drainFactory.noMoreOperators();
                        drainSource.noMoreInput();
                        drainJoinOutput(drain, drainSource, List.<Page>of().iterator(), types, context, memoryValidation);
                    }
                }
                else {
                    try (var probeSource = new SettableGpuOperation();
                            GpuOperation probe = probeFactory.create(context, probeSource)) {
                        probeOutputPageRows = drainJoinOutput(probe, probeSource, probePages, types, context, memoryValidation);
                    }
                    probeFactory.noMoreOperators();
                }

                // Drive build to Finished after all probes done
                memoryValidation.withoutValidation(() -> {
                    assertThat(build.execute()).isInstanceOf(Finished.class);
                });
            }
        }
        return probeOutputPageRows;
    }

    /// Returns the row counts of the pages `top` emits.
    private static List<Integer> drainJoinOutput(
            GpuOperation top,
            SettableGpuOperation probeSource,
            Iterator<Page> probePages,
            List<Type> types,
            TestingGpuOperationContext context,
            GpuDeviceMemoryUsageValidation memoryValidation)
    {
        List<Integer> pageRows = new ArrayList<>();
        boolean finished = false;
        while (!finished) {
            switch (top.execute()) {
                case Yielded() -> {
                    if (!probeSource.hasPending()) {
                        memoryValidation.withoutValidation(() -> {
                            if (probePages.hasNext()) {
                                GpuPage next = getOnlyElement(copyToDevice(List.of(probePages.next()), types));
                                probeSource.setPending(context.taskMemoryContext().allocate("probe", next.retainedMemory()), next);
                            }
                            else {
                                probeSource.noMoreInput();
                            }
                        });
                    }
                }
                case Data(AllocatedMemory memory, GpuPage page) -> {
                    pageRows.add(page.positionCount());
                    try (var closer = UncheckedCloser.create()) {
                        closer.register(memory);
                        closer.register(page);
                    }
                }
                case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                case Finished() -> finished = true;
            }
        }
        return pageRows;
    }
}
