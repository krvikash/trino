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

import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToDevice;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
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

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager();

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

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager();

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

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager();

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

    @Test
    void testInnerJoinEmptyBuild()
    {
        List<Type> buildTypes = List.of(BIGINT);
        List<Type> probeTypes = List.of(BIGINT);

        Page probePage = new Page(longBlock(1L, 2L, 3L));

        GpuJoinBridgeManager manager = new GpuJoinBridgeManager();

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
            this.input = buildPages.iterator();
            this.bufferPages = new BufferPages();
            Set<Integer> deviceChannels = IntStream.range(0, buildTypes.size()).boxed().collect(toImmutableSet());
            CopyToDevice copyToDevice = new CopyToDevice(bufferPages, buildTypes, deviceChannels);
            GpuJoinBuild.Factory factory = new GpuJoinBuild.Factory(manager, buildKeyChannels, buildOutputChannels, Optional.empty(), Optional.empty());
            this.build = factory.create(copyToDevice);
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
        GpuLookupJoin.Factory factory = new GpuLookupJoin.Factory(
                manager, probeKeyChannels, probeOutputChannels, joinType, buildOutputTypes, false);
        try {
            return executeGpuOperation(probePages, probeTypes, outputTypes, factory::create);
        }
        finally {
            factory.noMoreOperators();
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
}
