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
import ai.rapids.cudf.ast.ColumnReference;
import ai.rapids.cudf.ast.TableReference;
import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToBlocks;
import io.trino.operator.gpu.CopyToDevice;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.join.GpuLookupJoin.JoinType;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static ai.rapids.cudf.ast.BinaryOperator.LESS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.gpu.GpuTestUtils.drainToPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_ROWS;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.FULL;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.RIGHT;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

/// Correctness tests for the GPU RIGHT and FULL joins, which preserve the build (right) side. The probe side is
/// the left side, which a FULL join preserves too, and output columns are the probe output columns followed by the
/// build output columns.
final class TestGpuBuildOuterJoin
{
    private static final int[] CHANNEL_0 = {0};
    private static final int[] EMPTY_CHANNELS = {};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testRightJoinSingleKey()
    {
        // Matched (1), (3), (5) plus drained (null:7)
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L, 5L, 7L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L, 5L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            long total = output.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(total).isEqualTo(4);
        }
    }

    @Test
    void testRightJoinWithBuildOutput()
    {
        // Build key = col0, build output = col1; build key 5 matches no probe row
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L, 5L), longBlock(10L, 30L, 50L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:10", "3:30", "null:50");
        }
    }

    @Test
    void testRightJoinFilterRejectsKeyMatch()
    {
        // ON probe.key = build.key AND probe.val < build.val: build (1, 10) matches the key but fails the filter,
        // so it must be drained like (3, 40), which matches no key
        List<Type> types = List.of(BIGINT, BIGINT);
        Page buildPage = new Page(longBlock(1L, 1L, 2L, 3L), longBlock(10L, 20L, 30L, 40L));
        Page probePage = new Page(longBlock(1L, 2L), longBlock(15L, 5L));
        AstExpression filter = new BinaryOperation(
                LESS,
                new ColumnReference(1, TableReference.LEFT),
                new ColumnReference(1, TableReference.RIGHT));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, Optional.of(filter), List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, RIGHT, List.of(BIGINT), MAX_OUTPUT_CHUNK_ROWS, true);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, List.of(BIGINT));
            List<Page> output = new ArrayList<>(runProbe(joinFactory, types, types, List.of(probePage)));
            output.addAll(runOuterDrain(drainFactory, types));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:20", "2:30", "null:10", "null:40");
        }
    }

    @Test
    void testRightJoinAllBuildRowsMatched()
    {
        // Every build row matches, so the drain emits nothing
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(output.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(2);
            for (Page page : output) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertThat(page.getBlock(0).isNull(i)).isFalse();
                }
            }
        }
    }

    @Test
    void testRightJoinEmptyProbe()
    {
        // Every build row is unmatched and drained with a NULL probe
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of());

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("null:10", "null:30");
        }
    }

    @Test
    void testRightJoinNoProbeDrivers()
    {
        // No probe splits, so no probe driver is created; the outer driver must still drain every build row
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeOutputTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, RIGHT, List.of(BIGINT), MAX_OUTPUT_CHUNK_ROWS, false);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeOutputTypes);
            // Closing the factory releases the barrier bootstrap so the drain can fire
            joinFactory.noMoreOperators();

            List<Page> output = runOuterDrain(drainFactory, List.of(BIGINT, BIGINT));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("null:10", "null:30");
        }
    }

    @Test
    void testRightJoinEmptyBuild()
    {
        // An empty preserved side produces no rows regardless of the probe
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page probePage = new Page(longBlock(1L, 2L, 3L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of())) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(output.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(0);
        }
    }

    @Test
    void testRightJoinDuplicateBuildKeys()
    {
        // Two build rows share key 1; each build row must appear exactly once
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 1L, 3L), longBlock(10L, 11L, 30L));
        Page probePage = new Page(longBlock(1L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:10", "1:11", "null:30");
        }
    }

    @Test
    void testRightJoinMultipleProbePages()
    {
        // Matches accumulate across probe pages before the drain computes the complement once
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L, 3L, 4L, 5L), longBlock(10L, 20L, 30L, 40L, 50L));
        Page probePage1 = new Page(longBlock(1L, 2L));
        Page probePage2 = new Page(longBlock(4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage1, probePage2));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output))
                    .containsExactlyInAnyOrder("1:10", "2:20", "4:40", "null:30", "null:50");
        }
    }

    @Test
    void testRightJoinMultipleProbeMatchesSameBuildRow()
    {
        // A build row matched by several probe rows must not be drained, nor double-counted
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L), longBlock(10L, 20L));
        Page probePage = new Page(longBlock(1L, 1L, 1L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output))
                    .containsExactlyInAnyOrder("1:10", "1:10", "1:10", "null:20");
        }
    }

    @Test
    void testRightJoinNoProbeOutputColumns()
    {
        // COUNT(*)-style probe: no probe output columns, so the output has only the build column
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));
        Page probePage = new Page(longBlock(1L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    RIGHT,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    EMPTY_CHANNELS,
                    List.of(BIGINT),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            List<Long> buildValues = new ArrayList<>();
            for (Page page : output) {
                assertThat(page.getChannelCount()).isEqualTo(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertThat(page.getBlock(0).isNull(i)).isFalse();
                    buildValues.add(BIGINT.getLong(page.getBlock(0), i));
                }
            }
            assertThat(buildValues).containsExactlyInAnyOrder(10L, 30L);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRightJoinChunkedOutput(boolean filtered)
    {
        // Build keys 0-9 repeat 20 times and match 100 probe rows, build keys 10-14 match no probe row
        int chunkRows = 100;
        int matchedKeys = 10;
        int buildRows = 300;
        long[] buildKeys = new long[buildRows];
        long[] buildValues = new long[buildRows];
        for (int i = 0; i < buildRows; i++) {
            buildKeys[i] = i % 15;
            buildValues[i] = i;
        }
        long[] probeKeys = new long[100];
        long[] probeValues = new long[probeKeys.length];
        for (int i = 0; i < probeKeys.length; i++) {
            probeKeys[i] = i % matchedKeys;
            probeValues[i] = i * 3L;
        }
        // ON probe.key = build.key AND probe.value < build.value
        Optional<AstExpression> filter = Optional.empty();
        if (filtered) {
            filter = Optional.of(new BinaryOperation(
                    LESS,
                    new ColumnReference(1, TableReference.LEFT),
                    new ColumnReference(1, TableReference.RIGHT)));
        }

        List<String> expected = new ArrayList<>();
        boolean[] buildMatched = new boolean[buildRows];
        for (int probe = 0; probe < probeKeys.length; probe++) {
            for (int build = 0; build < buildRows; build++) {
                if (probeKeys[probe] == buildKeys[build] && (!filtered || probeValues[probe] < buildValues[build])) {
                    expected.add(probeKeys[probe] + ":" + buildValues[build]);
                    buildMatched[build] = true;
                }
            }
        }
        for (int build = 0; build < buildRows; build++) {
            if (!buildMatched[build]) {
                expected.add("null:" + buildValues[build]);
            }
        }
        assertThat(expected).hasSizeGreaterThan(5 * chunkRows);

        List<Type> types = List.of(BIGINT, BIGINT);
        Page buildPage = new Page(longBlock(buildKeys), longBlock(buildValues));
        Page probePage = new Page(longBlock(probeKeys), longBlock(probeValues));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);
        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, filter, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, RIGHT, List.of(BIGINT), chunkRows, filtered);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, List.of(BIGINT));
            List<Page> output = new ArrayList<>(runProbe(joinFactory, types, List.of(BIGINT, BIGINT), List.of(probePage)));
            output.addAll(runOuterDrain(drainFactory, List.of(BIGINT, BIGINT)));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testRightJoinAbortedProbeDriverDropsOuterSide()
    {
        List<Type> probeTypes = List.of(BIGINT);
        List<Type> outputTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, List.of(BIGINT), CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, RIGHT, List.of(), MAX_OUTPUT_CHUNK_ROWS, false);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeTypes);

            // Larger than BufferPages.TARGET_ROW_COUNT, so the driver records its match before its source finishes
            Page probePageA = new Page(constantLongBlock(1L, 150_000));
            Page probePageB = new Page(longBlock(3L));

            List<Page> innerOutput;
            try (ProbeDriver driverA = new ProbeDriver(joinFactory, probeTypes, outputTypes);
                    ProbeDriver driverB = new ProbeDriver(joinFactory, probeTypes, outputTypes)) {
                joinFactory.noMoreOperators();

                // Torn down before finishing
                driverA.feedAndRecordFirstPage(probePageA);
                driverA.close();

                // Finishes and merges its match; with a peer aborted the outer side is dropped
                innerOutput = driverB.runToCompletion(probePageB);
            }

            // The drain observes the abort and emits nothing
            List<Page> output = new ArrayList<>(innerOutput);
            output.addAll(runOuterDrain(drainFactory, outputTypes));

            assertThat(build.run()).isInstanceOf(Finished.class);

            // Only build key 3, matched by driver B
            List<Long> values = new ArrayList<>();
            for (Page page : output) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertThat(page.getBlock(0).isNull(i)).isFalse();
                    values.add(BIGINT.getLong(page.getBlock(0), i));
                }
            }
            assertThat(values).containsExactly(3L);
        }
    }

    @Test
    void testFullJoinSingleKey()
    {
        // Matched (1), (3), (5), probe rows (2), (4) with no build row, plus drained (null) for build key 7
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L, 5L, 7L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L, 5L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(bigintColumnValues(output, 0)).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, null);
        }
    }

    @Test
    void testFullJoinWithBuildOutput()
    {
        // Build key = col0, build output = col1; probe keys 2 and 4 match no build row, build key 5 matches no probe row
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L, 5L), longBlock(10L, 30L, 50L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:10", "2:null", "3:30", "4:null", "null:50");
        }
    }

    @Test
    void testFullJoinFilterRejectsKeyMatch()
    {
        // ON probe.key = build.key AND probe.val < build.val: build (1, 10) matches the key but fails the filter, so it
        // must be drained like (3, 40); probe (3, 50) matches the key of (3, 40) but fails the filter, so it is emitted
        // with a NULL build side
        List<Type> types = List.of(BIGINT, BIGINT);
        Page buildPage = new Page(longBlock(1L, 1L, 2L, 3L), longBlock(10L, 20L, 30L, 40L));
        Page probePage = new Page(longBlock(1L, 2L, 3L), longBlock(15L, 5L, 50L));
        AstExpression filter = new BinaryOperation(
                LESS,
                new ColumnReference(1, TableReference.LEFT),
                new ColumnReference(1, TableReference.RIGHT));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, Optional.of(filter), List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, FULL, List.of(BIGINT), MAX_OUTPUT_CHUNK_ROWS, true);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, List.of(BIGINT));
            List<Page> output = new ArrayList<>(runProbe(joinFactory, types, types, List.of(probePage)));
            output.addAll(runOuterDrain(drainFactory, types));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:20", "2:30", "3:null", "null:10", "null:40");
        }
    }

    @Test
    void testFullJoinAllBuildRowsMatched()
    {
        // Every build row matches, so the drain emits nothing; the probe rows with no match are still emitted
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(bigintColumnValues(output, 0)).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
        }
    }

    @Test
    void testFullJoinNoProbeRowMatched()
    {
        // Every build index in the gather map is out of bounds, so no build row is marked and all are drained
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));
        Page probePage = new Page(longBlock(2L, 4L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("2:null", "4:null", "4:null", "null:10", "null:30");
        }
    }

    @Test
    void testFullJoinEmptyProbe()
    {
        // Every build row is unmatched and drained with a NULL probe
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of());

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("null:10", "null:30");
        }
    }

    @Test
    void testFullJoinNoProbeDrivers()
    {
        // No probe splits, so no probe driver is created; the outer driver must still drain every build row
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeOutputTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, FULL, List.of(BIGINT), MAX_OUTPUT_CHUNK_ROWS, false);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeOutputTypes);
            // Closing the factory releases the barrier bootstrap so the drain can fire
            joinFactory.noMoreOperators();

            List<Page> output = runOuterDrain(drainFactory, List.of(BIGINT, BIGINT));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("null:10", "null:30");
        }
    }

    @Test
    void testFullJoinEmptyBuild()
    {
        // With no build rows, every probe row is emitted with a NULL build side and the drain emits nothing
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page probePage = new Page(longBlock(1L, 2L, 3L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of())) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:null", "2:null", "3:null");
        }
    }

    @Test
    void testFullJoinDuplicateBuildKeys()
    {
        // Two build rows share key 1; each build row must appear exactly once
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 1L, 3L), longBlock(10L, 11L, 30L));
        Page probePage = new Page(longBlock(1L, 2L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output)).containsExactlyInAnyOrder("1:10", "1:11", "2:null", "null:30");
        }
    }

    @Test
    void testFullJoinMultipleProbePages()
    {
        // Matches accumulate across probe pages before the drain computes the complement once
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L, 3L, 4L, 5L), longBlock(10L, 20L, 30L, 40L, 50L));
        Page probePage1 = new Page(longBlock(1L, 2L, 6L));
        Page probePage2 = new Page(longBlock(4L, 7L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage1, probePage2));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output))
                    .containsExactlyInAnyOrder("1:10", "2:20", "6:null", "4:40", "7:null", "null:30", "null:50");
        }
    }

    @Test
    void testFullJoinMultipleProbeMatchesSameBuildRow()
    {
        // A build row matched by several probe rows must not be drained, nor double-counted
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L), longBlock(10L, 20L));
        Page probePage = new Page(longBlock(1L, 1L, 3L, 1L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(probeKeyBuildValRows(output))
                    .containsExactlyInAnyOrder("1:10", "1:10", "1:10", "3:null", "null:20");
        }
    }

    @Test
    void testFullJoinNoProbeOutputColumns()
    {
        // COUNT(*)-style probe: no probe output columns, so the output has only the build column, which is NULL for
        // the probe row with no match
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));
        Page probePage = new Page(longBlock(1L, 2L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runBuildOuterJoin(
                    FULL,
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    EMPTY_CHANNELS,
                    List.of(BIGINT),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            for (Page page : output) {
                assertThat(page.getChannelCount()).isEqualTo(1);
            }
            assertThat(bigintColumnValues(output, 0)).containsExactlyInAnyOrder(10L, null, 30L);
        }
    }

    @Test
    void testFullJoinAbortedProbeDriverDropsOuterSide()
    {
        List<Type> probeTypes = List.of(BIGINT);
        List<Type> outputTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(true);

        try (BuildDriver build = new BuildDriver(manager, List.of(BIGINT), CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                    manager, CHANNEL_0, CHANNEL_0, FULL, List.of(), MAX_OUTPUT_CHUNK_ROWS, false);
            GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeTypes);

            // Larger than BufferPages.TARGET_ROW_COUNT, so the driver records its match before its source finishes
            Page probePageA = new Page(constantLongBlock(1L, 150_000));
            Page probePageB = new Page(longBlock(3L, 9L));

            List<Page> innerOutput;
            try (ProbeDriver driverA = new ProbeDriver(joinFactory, probeTypes, outputTypes);
                    ProbeDriver driverB = new ProbeDriver(joinFactory, probeTypes, outputTypes)) {
                joinFactory.noMoreOperators();

                // Torn down before finishing
                driverA.feedAndRecordFirstPage(probePageA);
                driverA.close();

                // Finishes and merges its match; with a peer aborted the outer build side is dropped
                innerOutput = driverB.runToCompletion(probePageB);
            }

            // The drain observes the abort and emits nothing
            List<Page> output = new ArrayList<>(innerOutput);
            output.addAll(runOuterDrain(drainFactory, outputTypes));

            assertThat(build.run()).isInstanceOf(Finished.class);

            // Build key 3, matched by driver B, and probe key 9, which matches no build row; no build row is drained
            assertThat(bigintColumnValues(output, 0)).containsExactlyInAnyOrder(3L, 9L);
        }
    }

    /// Runs the join as two drivers, mirroring the real plan: the probe output followed by the drained output.
    private static List<Page> runBuildOuterJoin(
            JoinType joinType,
            GpuJoinBridgeManager manager,
            List<Type> probeTypes,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            List<Type> outputTypes,
            List<Page> probePages)
    {
        List<Type> probeOutputTypes = IntStream.of(probeOutputChannels).mapToObj(probeTypes::get).collect(toImmutableList());
        GpuLookupJoin.Factory joinFactory = new GpuLookupJoin.Factory(
                manager, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, MAX_OUTPUT_CHUNK_ROWS, false);
        GpuJoinOuterDrain.Factory drainFactory = new GpuJoinOuterDrain.Factory(manager, probeOutputTypes);

        List<Page> output = new ArrayList<>(runProbe(joinFactory, probeTypes, outputTypes, probePages));
        output.addAll(runOuterDrain(drainFactory, outputTypes));
        return output;
    }

    /// Drives one probe driver to completion. Closing the factory once the single operator exists drops the
    /// barrier bootstrap, mirroring a real single-driver plan.
    private static List<Page> runProbe(GpuLookupJoin.Factory joinFactory, List<Type> probeTypes, List<Type> outputTypes, List<Page> probePages)
    {
        Set<Integer> deviceChannels = IntStream.range(0, probeTypes.size()).boxed().collect(toImmutableSet());
        Iterator<Page> input = probePages.iterator();
        GpuOperation.Context context = new TestingGpuOperationContext();
        try (BufferPages bufferPages = new BufferPages(context.taskMemoryContext());
                CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, probeTypes, deviceChannels);
                GpuOperation join = joinFactory.create(context, copyToDevice);
                CopyToBlocks copyToBlocks = new CopyToBlocks(context, join, outputTypes)) {
            joinFactory.noMoreOperators();
            return drainToPages(
                    () -> {
                        if (!input.hasNext()) {
                            bufferPages.noMoreInput();
                        }
                        else if (bufferPages.needsInput()) {
                            bufferPages.addInput(input.next());
                        }
                    },
                    copyToBlocks);
        }
    }

    /// Drives the single outer driver. Callers must run it only once the build side is published and every
    /// probe has finished, so the drain never reports {@link Blocked}.
    private static List<Page> runOuterDrain(GpuJoinOuterDrain.Factory drainFactory, List<Type> outputTypes)
    {
        GpuOperation.Context context = new TestingGpuOperationContext();
        try (BufferPages source = new BufferPages(context.taskMemoryContext());
                GpuOperation drain = drainFactory.create(context, source);
                CopyToBlocks copyToBlocks = new CopyToBlocks(context, drain, outputTypes)) {
            drainFactory.noMoreOperators();
            source.noMoreInput();
            return drainToPages(() -> {}, copyToBlocks);
        }
    }

    /// Flattens (probeKey, buildVal) pages into `"probeKey:buildVal"` strings. Returns a list, not a set, so
    /// duplicate rows are asserted faithfully.
    private static List<String> probeKeyBuildValRows(List<Page> output)
    {
        List<String> rows = new ArrayList<>();
        for (Page page : output) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                String probeKey = page.getBlock(0).isNull(i) ? "null" : String.valueOf(BIGINT.getLong(page.getBlock(0), i));
                String buildVal = page.getBlock(1).isNull(i) ? "null" : String.valueOf(BIGINT.getLong(page.getBlock(1), i));
                rows.add(probeKey + ":" + buildVal);
            }
        }
        return rows;
    }

    /// Flattens one BIGINT column of the pages, with `null` for NULL positions.
    private static List<Long> bigintColumnValues(List<Page> output, int channel)
    {
        List<Long> values = new ArrayList<>();
        for (Page page : output) {
            Block block = page.getBlock(channel);
            for (int i = 0; i < page.getPositionCount(); i++) {
                values.add(block.isNull(i) ? null : BIGINT.getLong(block, i));
            }
        }
        return values;
    }

    /// Drives a {@link GpuJoinBuild} pipeline. Tests must call {@link #run} once before running probes, to
    /// publish the bridge, and again after they finish, to advance the build to {@link Finished}.
    private static final class BuildDriver
            implements AutoCloseable
    {
        private final BufferPages bufferPages;
        private final Iterator<Page> input;
        private final GpuOperation build;

        BuildDriver(GpuJoinBridgeManager manager, List<Type> buildTypes, int[] buildKeyChannels, int[] buildOutputChannels, List<Page> buildPages)
        {
            this(manager, buildTypes, buildKeyChannels, buildOutputChannels, Optional.empty(), buildPages);
        }

        BuildDriver(GpuJoinBridgeManager manager, List<Type> buildTypes, int[] buildKeyChannels, int[] buildOutputChannels, Optional<AstExpression> filter, List<Page> buildPages)
        {
            this.input = buildPages.iterator();
            GpuOperation.Context context = new TestingGpuOperationContext();
            this.bufferPages = new BufferPages(context.taskMemoryContext());
            Set<Integer> deviceChannels = IntStream.range(0, buildTypes.size()).boxed().collect(toImmutableSet());
            CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, buildTypes, deviceChannels);
            GpuJoinBuild.Factory factory = new GpuJoinBuild.Factory(manager, buildKeyChannels, buildOutputChannels, filter, Optional.empty());
            this.build = factory.create(context, copyToDevice);
        }

        /// Drives build until it neither yields nor produces data.
        @Move
        GpuOperation.Result run()
        {
            while (true) {
                if (!input.hasNext()) {
                    bufferPages.noMoreInput();
                }
                else if (bufferPages.needsInput()) {
                    bufferPages.addInput(input.next());
                }
                @Own GpuOperation.Result result = build.execute();
                switch (result) {
                    case Blocked blocked -> {
                        return blocked;
                    }
                    case Data _ -> throw new IllegalStateException("Build should never emit data");
                    case Yielded _ -> {
                        /* continue */
                    }
                    case Finished finished -> {
                        return finished;
                    }
                }
            }
        }

        @Override
        public void close()
        {
            build.close();
        }
    }

    private static Block longBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long v : values) {
            BIGINT.writeLong(builder, v);
        }
        return builder.build();
    }

    private static Block constantLongBlock(long value, int count)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    /// One probe driver, driven either by {@link #feedAndRecordFirstPage}, which leaves the source
    /// unfinished so {@link #close} is an abort, or by {@link #runToCompletion}.
    private static final class ProbeDriver
            implements AutoCloseable
    {
        private final BufferPages bufferPages;
        private final CopyToBlocks copyToBlocks;
        private boolean closed;

        ProbeDriver(GpuLookupJoin.Factory joinFactory, List<Type> probeTypes, List<Type> outputTypes)
        {
            GpuOperation.Context context = new TestingGpuOperationContext();
            Set<Integer> deviceChannels = IntStream.range(0, probeTypes.size()).boxed().collect(toImmutableSet());
            this.bufferPages = new BufferPages(context.taskMemoryContext());
            CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, probeTypes, deviceChannels);
            GpuOperation join = joinFactory.create(context, copyToDevice);
            this.copyToBlocks = new CopyToBlocks(context, join, outputTypes);
        }

        /// Steps until the first Data page, by which point the join has recorded that page's matched build
        /// rows. The source is left unfinished, so a subsequent {@link #close} aborts with matches not handed off.
        void feedAndRecordFirstPage(Page page)
        {
            bufferPages.addInput(page);
            while (true) {
                @Own GpuOperation.Result result = copyToBlocks.execute();
                switch (result) {
                    case Data(AllocatedMemory memory, GpuPage gpuPage) -> {
                        try (memory; gpuPage) {
                            return;
                        }
                    }
                    case Yielded _ -> {
                        /* keep loading the device page */
                    }
                    case Blocked _ -> throw new IllegalStateException("Pipeline blocked before recording matches");
                    case Finished _ -> throw new IllegalStateException("Pipeline finished before recording matches");
                }
            }
        }

        List<Page> runToCompletion(Page page)
        {
            Iterator<Page> input = List.of(page).iterator();
            return drainToPages(
                    () -> {
                        if (!input.hasNext()) {
                            bufferPages.noMoreInput();
                        }
                        else if (bufferPages.needsInput()) {
                            bufferPages.addInput(input.next());
                        }
                    },
                    copyToBlocks);
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            try {
                copyToBlocks.close();
            }
            finally {
                bufferPages.close();
            }
        }
    }
}
