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
package io.trino.operator.torin.join;

import ai.rapids.cudf.ast.AstExpression;
import ai.rapids.cudf.ast.BinaryOperation;
import ai.rapids.cudf.ast.BinaryOperator;
import ai.rapids.cudf.ast.ColumnReference;
import ai.rapids.cudf.ast.TableReference;
import io.airlift.slice.Slices;
import io.trino.operator.torin.BufferPages;
import io.trino.operator.torin.CopyToDevice;
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.GpuOperation.Blocked;
import io.trino.operator.torin.GpuOperation.Data;
import io.trino.operator.torin.GpuOperation.Finished;
import io.trino.operator.torin.GpuOperation.Yielded;
import io.trino.operator.torin.TestingGpuOperationContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ai.rapids.cudf.ast.BinaryOperator.GREATER;
import static ai.rapids.cudf.ast.BinaryOperator.GREATER_EQUAL;
import static ai.rapids.cudf.ast.BinaryOperator.LESS;
import static ai.rapids.cudf.ast.BinaryOperator.LESS_EQUAL;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.torin.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.join.GpuJoinUtils.MAX_OUTPUT_CHUNK_ROWS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static java.lang.Math.ceilDiv;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuLookupJoin
{
    private static final int[] CHANNEL_0 = {0};
    private static final int[] EMPTY_CHANNELS = {};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testInnerJoinSingleKey()
    {
        // Build = (1), (3), (5)
        // Probe = (1), (2), (3), (4), (5)
        // Expected: (1), (3), (5)
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L, 5L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L, 5L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runProbe(
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    GpuLookupJoin.JoinType.INNER,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            long total = output.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(total).isEqualTo(3);
        }
    }

    @Test
    void testLeftJoinSingleKey()
    {
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runProbe(
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    GpuLookupJoin.JoinType.LEFT,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            long total = output.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(total).isEqualTo(4);
        }
    }

    @Test
    void testLeftJoinWithBuildOutput()
    {
        // Build key = col0, build output = col1
        // Build = (key=1, val=10), (key=3, val=30)
        // Probe key = col0, probe output = col0
        // Probe = (1), (2), (3), (4)
        // Expected LEFT JOIN output (probeKey, buildVal): (1,10), (2,null), (3,30), (4,null)
        List<Type> buildTypes = List.of(BIGINT, BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page buildPage = new Page(longBlock(1L, 3L), longBlock(10L, 30L));
        Page probePage = new Page(longBlock(1L, 2L, 3L, 4L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runProbe(
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    GpuLookupJoin.JoinType.LEFT,
                    List.of(BIGINT),
                    List.of(BIGINT, BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(output.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(4);

            // Collect all (probeKey, buildVal) pairs
            Set<String> rows = new HashSet<>();
            for (Page page : output) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long probeKey = BIGINT.getLong(page.getBlock(0), i);
                    String buildVal = page.getBlock(1).isNull(i) ? "null" : String.valueOf(BIGINT.getLong(page.getBlock(1), i));
                    rows.add(probeKey + ":" + buildVal);
                }
            }
            assertThat(rows).containsExactlyInAnyOrder("1:10", "2:null", "3:30", "4:null");
        }
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("charFilterCases")
    void testFilteredCharComparison(GpuLookupJoin.JoinType joinType, BinaryOperator operator, List<String> expectedMatched, List<String> expectedUnmatched)
    {
        CharType charType = createCharType(2);
        List<Type> types = List.of(BIGINT, charType);
        Page buildPage = new Page(longBlock(1L), charBlock(charType, "a\t"));
        Page probePage = new Page(longBlock(1L, 1L, 1L), charBlock(charType, "a", "a\t", "b"));
        AstExpression filter = new BinaryOperation(
                operator,
                new ColumnReference(1, TableReference.LEFT),
                new ColumnReference(1, TableReference.RIGHT));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);
        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, Optional.of(filter), List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);
            List<Page> output = runProbe(
                    manager,
                    types,
                    CHANNEL_0,
                    new int[] {0, 1},
                    joinType,
                    List.of(charType),
                    List.of(BIGINT, charType, charType),
                    true,
                    List.of(probePage));
            assertThat(build.run()).isInstanceOf(Finished.class);

            List<String> matchedProbes = new ArrayList<>();
            List<String> unmatchedProbes = new ArrayList<>();
            for (Page page : output) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(1);
                    String probeValue = charType.getSlice(page.getBlock(1), position).toStringUtf8();
                    if (page.getBlock(2).isNull(position)) {
                        unmatchedProbes.add(probeValue);
                    }
                    else {
                        assertThat(charType.getSlice(page.getBlock(2), position).toStringUtf8()).isEqualTo("a\t");
                        matchedProbes.add(probeValue);
                    }
                }
            }

            assertThat(matchedProbes).containsExactlyInAnyOrderElementsOf(expectedMatched);
            assertThat(unmatchedProbes).containsExactlyInAnyOrderElementsOf(expectedUnmatched);
        }
    }

    static Stream<Arguments> charFilterCases()
    {
        // CHAR(2) compares "a" as "a ", so "a\t" sorts before "a" and "b".
        return Stream.of(
                Arguments.of(GpuLookupJoin.JoinType.INNER, LESS, List.of(), List.of()),
                Arguments.of(GpuLookupJoin.JoinType.INNER, LESS_EQUAL, List.of("a\t"), List.of()),
                Arguments.of(GpuLookupJoin.JoinType.INNER, GREATER, List.of("a", "b"), List.of()),
                Arguments.of(GpuLookupJoin.JoinType.INNER, GREATER_EQUAL, List.of("a", "a\t", "b"), List.of()),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, LESS, List.of(), List.of("a", "a\t", "b")),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, LESS_EQUAL, List.of("a\t"), List.of("a", "b")),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, GREATER, List.of("a", "b"), List.of("a\t")),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, GREATER_EQUAL, List.of("a", "a\t", "b"), List.of()));
    }

    @Test
    void testInnerJoinEmptyBuild()
    {
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page probePage = new Page(longBlock(1L, 2L, 3L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);

        try (BuildDriver build = new BuildDriver(manager, buildTypes, CHANNEL_0, EMPTY_CHANNELS, List.of())) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Page> output = runProbe(
                    manager,
                    probeTypes,
                    CHANNEL_0,
                    CHANNEL_0,
                    GpuLookupJoin.JoinType.INNER,
                    List.of(),
                    List.of(BIGINT),
                    List.of(probePage));

            assertThat(build.run()).isInstanceOf(Finished.class);

            long total = output.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(total).isEqualTo(0);
        }
    }

    @ParameterizedTest(name = "{0} filtered={1}")
    @MethodSource("chunkedJoinCases")
    void testHighFanOutJoinIsChunked(GpuLookupJoin.JoinType joinType, boolean filtered)
    {
        // Every build key repeats buildRowsPerKey times, so a single probe page has a high fan-out
        int chunkRows = 1_000;
        int buildKeys = 50;
        int buildRowsPerKey = 40;
        int probeRows = 1_000;
        // Probe keys 50-59 match no build key
        int probeKeys = 60;

        long[] buildKeyValues = new long[buildKeys * buildRowsPerKey];
        long[] buildValues = new long[buildKeyValues.length];
        for (int i = 0; i < buildKeyValues.length; i++) {
            buildKeyValues[i] = i % buildKeys;
            buildValues[i] = i;
        }
        long[] probeKeyValues = new long[probeRows];
        long[] probeValues = new long[probeRows];
        for (int i = 0; i < probeRows; i++) {
            probeKeyValues[i] = i % probeKeys;
            // Spread over the build values, so the filter below keeps only some matches of a probe row
            probeValues[i] = i * 2L;
        }
        // ON probe.key = build.key AND probe.value < build.value
        Optional<AstExpression> filter = Optional.empty();
        if (filtered) {
            filter = Optional.of(new BinaryOperation(
                    LESS,
                    new ColumnReference(1, TableReference.LEFT),
                    new ColumnReference(1, TableReference.RIGHT)));
        }

        List<Type> types = List.of(BIGINT, BIGINT);
        Page buildPage = new Page(longBlock(buildKeyValues), longBlock(buildValues));
        Page probePage = new Page(longBlock(probeKeyValues), longBlock(probeValues));

        List<String> expected = new ArrayList<>();
        for (int probe = 0; probe < probeRows; probe++) {
            boolean matched = false;
            for (int build = 0; build < buildKeyValues.length; build++) {
                if (probeKeyValues[probe] == buildKeyValues[build] && (!filtered || probeValues[probe] < buildValues[build])) {
                    expected.add(probeKeyValues[probe] + ":" + probeValues[probe] + ":" + buildValues[build]);
                    matched = true;
                }
            }
            if (!matched && joinType == GpuLookupJoin.JoinType.LEFT) {
                expected.add(probeKeyValues[probe] + ":" + probeValues[probe] + ":null");
            }
        }

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);
        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, filter, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Integer> joinPageRows = new ArrayList<>();
            List<Page> output = runProbe(
                    new GpuLookupJoin.Factory(manager, CHANNEL_0, new int[] {0, 1}, joinType, List.of(BIGINT), chunkRows, filtered),
                    types,
                    List.of(BIGINT, BIGINT, BIGINT),
                    List.of(probePage),
                    joinPageRows);

            assertThat(build.run()).isInstanceOf(Finished.class);

            assertThat(expected).hasSizeGreaterThan(5 * chunkRows);
            assertThat(joinPageRows)
                    .hasSize(ceilDiv(expected.size(), chunkRows))
                    .allSatisfy(rows -> assertThat(rows).isBetween(1, chunkRows));
            assertThat(probeValueBuildValueRows(output)).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    static Stream<Arguments> chunkedJoinCases()
    {
        return Stream.of(
                Arguments.of(GpuLookupJoin.JoinType.INNER, false),
                Arguments.of(GpuLookupJoin.JoinType.INNER, true),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, false),
                Arguments.of(GpuLookupJoin.JoinType.LEFT, true));
    }

    @ParameterizedTest
    @EnumSource(value = GpuLookupJoin.JoinType.class, names = {"INNER", "LEFT"})
    void testSingleProbeRowExceedingChunkLimit(GpuLookupJoin.JoinType joinType)
    {
        // Probe rows with key 7 match every build row, more than a chunk may hold
        int chunkRows = 100;
        int buildRows = 300;
        long[] buildKeys = new long[buildRows];
        long[] buildValues = new long[buildRows];
        for (int i = 0; i < buildRows; i++) {
            buildKeys[i] = 7;
            buildValues[i] = i;
        }
        List<Type> types = List.of(BIGINT, BIGINT);
        Page buildPage = new Page(longBlock(buildKeys), longBlock(buildValues));
        Page probePage = new Page(longBlock(7L, 1L, 7L, 2L), longBlock(10L, 11L, 12L, 13L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager(false);
        try (BuildDriver build = new BuildDriver(manager, types, CHANNEL_0, new int[] {1}, List.of(buildPage))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            List<Integer> joinPageRows = new ArrayList<>();
            List<Page> output = runProbe(
                    new GpuLookupJoin.Factory(manager, CHANNEL_0, new int[] {0, 1}, joinType, List.of(BIGINT), chunkRows, false),
                    types,
                    List.of(BIGINT, BIGINT, BIGINT),
                    List.of(probePage),
                    joinPageRows);

            assertThat(build.run()).isInstanceOf(Finished.class);

            List<String> expected = new ArrayList<>();
            for (long probeValue : List.of(10L, 12L)) {
                for (int i = 0; i < buildRows; i++) {
                    expected.add("7:" + probeValue + ":" + i);
                }
            }
            if (joinType == GpuLookupJoin.JoinType.LEFT) {
                expected.add("1:11:null");
                expected.add("2:13:null");
                assertThat(joinPageRows).containsExactly(100, 100, 100, 100, 100, 100, 2);
            }
            else {
                assertThat(joinPageRows).containsExactly(100, 100, 100, 100, 100, 100);
            }
            assertThat(probeValueBuildValueRows(output)).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * Drives a {@link GpuJoinBuild} pipeline. After buffering all input and publishing the
     * bridge, the build operation reports {@link Blocked} until every probe
     * operator has closed. Tests must call {@link #run} once before running probes (to publish
     * the bridge) and again after probes finish (to advance the build to {@link Finished}).
     */
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

        /**
         * Drives build until it neither yields nor produces data.
         */
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

    private static List<Page> runProbe(
            GpuJoinBridgeManager manager,
            List<Type> probeTypes,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            GpuLookupJoin.JoinType joinType,
            List<Type> buildOutputTypes,
            List<Type> outputTypes,
            List<Page> probePages)
    {
        return runProbe(manager, probeTypes, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, outputTypes, false, probePages);
    }

    private static List<Page> runProbe(
            GpuJoinBridgeManager manager,
            List<Type> probeTypes,
            int[] probeKeyChannels,
            int[] probeOutputChannels,
            GpuLookupJoin.JoinType joinType,
            List<Type> buildOutputTypes,
            List<Type> outputTypes,
            boolean filteredJoin,
            List<Page> probePages)
    {
        GpuLookupJoin.Factory factory = new GpuLookupJoin.Factory(
                manager, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, MAX_OUTPUT_CHUNK_ROWS, filteredJoin);
        try {
            return executeGpuOperation(probePages, probeTypes, outputTypes, factory::create);
        }
        finally {
            factory.noMoreOperators();
        }
    }

    /// Runs the probe side, recording the row count of every page the join emits.
    private static List<Page> runProbe(
            GpuLookupJoin.Factory factory,
            List<Type> probeTypes,
            List<Type> outputTypes,
            List<Page> probePages,
            List<Integer> joinPageRows)
    {
        try {
            return executeGpuOperation(
                    probePages,
                    probeTypes,
                    outputTypes,
                    (context, source) -> new PageRowsRecorder(factory.create(context, source), joinPageRows));
        }
        finally {
            factory.noMoreOperators();
        }
    }

    /// Flattens (probeKey, probeValue, buildValue) pages into `"probeKey:probeValue:buildValue"` strings.
    private static List<String> probeValueBuildValueRows(List<Page> output)
    {
        List<String> rows = new ArrayList<>();
        for (Page page : output) {
            for (int i = 0; i < page.getPositionCount(); i++) {
                String buildValue = page.getBlock(2).isNull(i) ? "null" : String.valueOf(BIGINT.getLong(page.getBlock(2), i));
                rows.add(BIGINT.getLong(page.getBlock(0), i) + ":" + BIGINT.getLong(page.getBlock(1), i) + ":" + buildValue);
            }
        }
        return rows;
    }

    /// Passes the results of `delegate` through, recording the row count of every page it emits.
    private record PageRowsRecorder(GpuOperation delegate, List<Integer> pageRows)
            implements GpuOperation
    {
        @Override
        public @Move Result execute()
        {
            @Own Result result = delegate.execute();
            if (result instanceof Data(_, GpuPage page)) {
                pageRows.add(page.positionCount());
            }
            return result;
        }

        @Override
        public void close()
        {
            delegate.close();
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

    private static Block charBlock(CharType charType, String... values)
    {
        BlockBuilder builder = charType.createBlockBuilder(null, values.length);
        for (String v : values) {
            charType.writeSlice(builder, Slices.utf8Slice(v));
        }
        return builder.build();
    }
}
