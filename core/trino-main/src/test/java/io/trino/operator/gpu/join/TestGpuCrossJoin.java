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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToBlocks;
import io.trino.operator.gpu.CopyToDevice;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.GpuPageToPages;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuCrossJoin
{
    private static final int[] NO_CHANNELS = {};
    private static final int[] CHANNEL_0 = {0};
    private static final int[] CHANNELS_0_AND_1 = {0, 1};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testBothSidesSingleRow()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(7L))),
                CHANNEL_0,
                List.of(new Page(longBlock(9L))),
                CHANNEL_0);

        assertThat(pairs(output)).isEqualTo(ImmutableMultiset.of("7:9"));
    }

    @Test
    void testProduct()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(1L, 2L)), new Page(longBlock(3L))),
                CHANNEL_0,
                List.of(new Page(longBlock(10L, 20L)), new Page(longBlock(30L))),
                CHANNEL_0);

        assertThat(pairs(output)).isEqualTo(ImmutableMultiset.of(
                "1:10",
                "1:20",
                "1:30",
                "2:10",
                "2:20",
                "2:30",
                "3:10",
                "3:20",
                "3:30"));
    }

    @Test
    void testEmptyProbe()
    {
        List<Page> output = crossJoin(
                List.of(),
                CHANNEL_0,
                List.of(new Page(longBlock(10L, 20L))),
                CHANNEL_0);

        assertThat(positionCount(output)).isEqualTo(0);
    }

    @Test
    void testEmptyBuild()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(1L, 2L))),
                CHANNEL_0,
                List.of(),
                CHANNEL_0);

        assertThat(positionCount(output)).isEqualTo(0);
    }

    @Test
    void testBothSidesEmpty()
    {
        List<Page> output = crossJoin(List.of(), CHANNEL_0, List.of(), CHANNEL_0);

        assertThat(positionCount(output)).isEqualTo(0);
    }

    @Test
    void testEmptyPages()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock()), new Page(longBlock(1L))),
                CHANNEL_0,
                List.of(new Page(longBlock()), new Page(longBlock(10L, 20L))),
                CHANNEL_0);

        assertThat(pairs(output)).isEqualTo(ImmutableMultiset.of("1:10", "1:20"));
    }

    @Test
    void testNulls()
    {
        List<Page> output = crossJoin(
                List.of(new Page(nullableLongBlock(1L, null))),
                CHANNEL_0,
                List.of(new Page(nullableLongBlock(null, 20L))),
                CHANNEL_0);

        assertThat(pairs(output)).isEqualTo(ImmutableMultiset.of("1:null", "1:20", "null:null", "null:20"));
    }

    @Test
    void testNoOutputChannels()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(1L, 2L, 3L))),
                NO_CHANNELS,
                List.of(new Page(longBlock(10L, 20L))),
                NO_CHANNELS);

        assertThat(output).allSatisfy(page -> assertThat(page.getChannelCount()).isEqualTo(0));
        assertThat(positionCount(output)).isEqualTo(6);
    }

    @Test
    void testProbeOutputChannelsOnly()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(1L, 2L))),
                CHANNEL_0,
                List.of(new Page(longBlock(10L, 20L, 30L))),
                NO_CHANNELS);

        assertThat(singleColumn(output)).isEqualTo(ImmutableMultiset.of("1", "1", "1", "2", "2", "2"));
    }

    @Test
    void testBuildOutputChannelsOnly()
    {
        List<Page> output = crossJoin(
                List.of(new Page(longBlock(1L, 2L))),
                NO_CHANNELS,
                List.of(new Page(longBlock(10L, 20L, 30L))),
                CHANNEL_0);

        assertThat(singleColumn(output)).isEqualTo(ImmutableMultiset.of("10", "10", "20", "20", "30", "30"));
    }

    @Test
    void testVarcharProduct()
    {
        List<Page> output = crossJoin(
                List.of(VARCHAR),
                List.of(new Page(varcharBlock("a", null))),
                CHANNEL_0,
                List.of(VARCHAR),
                List.of(new Page(varcharBlock("x", null, "zzz"))),
                CHANNEL_0);

        assertThat(rows(output, List.of(VARCHAR, VARCHAR))).isEqualTo(ImmutableMultiset.of(
                "a:x",
                "a:null",
                "a:zzz",
                "null:x",
                "null:null",
                "null:zzz"));
    }

    /// Two variable-width columns whose nulls fall in different rows, one of them VARBINARY. Finding
    /// the widest row has to reduce past the nulls and count a VARBINARY row as a list of bytes rather
    /// than as a string, and neither may disturb the product.
    @Test
    void testTwoVariableWidthColumnsWithNullsInDifferentRows()
    {
        List<Page> output = crossJoin(
                List.of(VARCHAR, VARBINARY),
                List.of(new Page(varcharBlock("a", null, "ccc"), varbinaryBlock(null, "22", "3"))),
                CHANNELS_0_AND_1,
                List.of(VARCHAR),
                List.of(new Page(varcharBlock("p", "qq"))),
                CHANNEL_0);

        assertThat(rows(output, List.of(VARCHAR, VARBINARY, VARCHAR))).isEqualTo(ImmutableMultiset.of(
                "a:null:p",
                "a:null:qq",
                "null:22:p",
                "null:22:qq",
                "ccc:3:p",
                "ccc:3:qq"));
    }

    /// Probe rows times build rows exceeds the chunk row cap, and the build row count does not
    /// divide that cap, so a chunk boundary falls in the middle of a probe row's output.
    @Test
    void testOutputSplitAcrossChunks()
    {
        int probeRows = 10_001;
        int buildRows = 1_001;

        ChunkedOutput output = crossJoinInChunks(
                List.of(new Page(sequenceBlock(probeRows))),
                CHANNEL_0,
                List.of(new Page(sequenceBlock(buildRows))),
                CHANNEL_0);

        assertThat(output.chunkCount()).isGreaterThan(1);
        assertThat(positionCount(output.pages())).isEqualTo((long) probeRows * buildRows);

        long probeSum = 0;
        long buildSum = 0;
        for (Page page : output.pages()) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                probeSum += BIGINT.getLong(page.getBlock(0), position);
                buildSum += BIGINT.getLong(page.getBlock(1), position);
            }
        }
        assertThat(probeSum).isEqualTo((long) buildRows * sequenceSum(probeRows));
        assertThat(buildSum).isEqualTo((long) probeRows * sequenceSum(buildRows));
    }

    /// Several probe pages, the first of them split across chunks, so the emitted row count restarts
    /// with every page while the build side stays in place.
    @Test
    void testOutputSplitAcrossChunksOfSeveralProbePages()
    {
        int firstProbeRows = 10_001;
        int secondProbeRows = 1_500;
        int buildRows = 1_001;

        ChunkedOutput output = crossJoinInChunks(
                List.of(new Page(sequenceBlock(firstProbeRows)), new Page(sequenceBlock(secondProbeRows))),
                CHANNEL_0,
                List.of(new Page(sequenceBlock(buildRows))),
                CHANNEL_0);

        assertThat(output.chunkCount()).isGreaterThan(2);
        assertThat(positionCount(output.pages())).isEqualTo((long) (firstProbeRows + secondProbeRows) * buildRows);

        long probeSum = 0;
        long buildSum = 0;
        for (Page page : output.pages()) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                probeSum += BIGINT.getLong(page.getBlock(0), position);
                buildSum += BIGINT.getLong(page.getBlock(1), position);
            }
        }
        assertThat(probeSum).isEqualTo((long) buildRows * (sequenceSum(firstProbeRows) + sequenceSum(secondProbeRows)));
        assertThat(buildSum).isEqualTo((long) (firstProbeRows + secondProbeRows) * sequenceSum(buildRows));
    }

    /// Two probe operators created from duplicated factories share one build side. The build side is
    /// released only once both are done with it, which a single probe operator never shows.
    @Test
    void testDuplicatedProbeFactoriesShareBuildSide()
    {
        List<Type> types = List.of(BIGINT);
        List<Type> outputTypes = outputTypes(types, CHANNEL_0, types, CHANNEL_0);
        GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();

        try (BuildDriver build = new BuildDriver(buildSideSupplier, types, CHANNEL_0, List.of(new Page(longBlock(10L, 20L))))) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuCrossJoin.Factory factory = new GpuCrossJoin.Factory(buildSideSupplier, CHANNEL_0);
            GpuCrossJoin.Factory duplicate = factory.duplicate();

            List<Page> first = executeGpuOperation(List.of(new Page(longBlock(1L))), types, outputTypes, factory::create);
            List<Page> second = executeGpuOperation(List.of(new Page(longBlock(2L, 3L))), types, outputTypes, duplicate::create);

            assertThat(pairs(first)).isEqualTo(ImmutableMultiset.of("1:10", "1:20"));
            assertThat(pairs(second)).isEqualTo(ImmutableMultiset.of("2:10", "2:20", "3:10", "3:20"));

            // Both factories hold the build side; closing one of them is not enough to release it
            factory.noMoreOperators();
            assertThat(build.run()).isInstanceOf(Blocked.class);
            duplicate.noMoreOperators();
            assertThat(build.run()).isInstanceOf(Finished.class);
        }
    }

    private record ChunkedOutput(int chunkCount, List<Page> pages) {}

    /// Drives the probe directly so the number of emitted [Data] results, that is the number of
    /// output chunks, is observable.
    private static ChunkedOutput crossJoinInChunks(List<Page> probePages, int[] probeOutputChannels, List<Page> buildPages, int[] buildOutputChannels)
    {
        List<Type> types = List.of(BIGINT);
        GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();

        try (BuildDriver build = new BuildDriver(buildSideSupplier, types, buildOutputChannels, buildPages)) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuOperation.Context context = new TestingGpuOperationContext();
            GpuCrossJoin.Factory factory = new GpuCrossJoin.Factory(buildSideSupplier, probeOutputChannels);
            GpuPageToPages gpuPageToPages = new GpuPageToPages();
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            int chunkCount = 0;
            Iterator<Page> probeInput = probePages.iterator();
            try (var probeSource = new SettableGpuOperation();
                    GpuOperation probe = factory.create(context, probeSource);
                    CopyToBlocks copyToBlocks = new CopyToBlocks(context, probe, outputTypes(types, probeOutputChannels, types, buildOutputChannels))) {
                boolean finished = false;
                while (!finished) {
                    gpuPageToPages.drain().forEachOrdered(pages::add);
                    @Own GpuOperation.Result result = copyToBlocks.execute();
                    switch (result) {
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                        case Data(AllocatedMemory memory, GpuPage page) -> {
                            chunkCount++;
                            try (memory; page) {
                                gpuPageToPages.add(page);
                            }
                        }
                        case Yielded _ -> {
                            if (!probeSource.hasPending()) {
                                if (probeInput.hasNext()) {
                                    GpuPage deviceProbePage = getOnlyElement(copyToDevice(List.of(probeInput.next()), types));
                                    probeSource.setPending(context.taskMemoryContext().allocate("probe", deviceProbePage.retainedMemory()), deviceProbePage);
                                }
                                else {
                                    probeSource.noMoreInput();
                                }
                            }
                        }
                        case Finished _ -> finished = true;
                    }
                }
                gpuPageToPages.drain().forEachOrdered(pages::add);
            }
            finally {
                factory.noMoreOperators();
            }

            assertThat(build.run()).isInstanceOf(Finished.class);
            return new ChunkedOutput(chunkCount, pages.build());
        }
    }

    private static List<Type> outputTypes(List<Type> probeTypes, int[] probeOutputChannels, List<Type> buildTypes, int[] buildOutputChannels)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : probeOutputChannels) {
            types.add(probeTypes.get(channel));
        }
        for (int channel : buildOutputChannels) {
            types.add(buildTypes.get(channel));
        }
        return types.build();
    }

    private static long sequenceSum(int rows)
    {
        return (long) rows * (rows - 1) / 2;
    }

    private static List<Page> crossJoin(List<Page> probePages, int[] probeOutputChannels, List<Page> buildPages, int[] buildOutputChannels)
    {
        return crossJoin(List.of(BIGINT), probePages, probeOutputChannels, List.of(BIGINT), buildPages, buildOutputChannels);
    }

    private static List<Page> crossJoin(
            List<Type> probeTypes,
            List<Page> probePages,
            int[] probeOutputChannels,
            List<Type> buildTypes,
            List<Page> buildPages,
            int[] buildOutputChannels)
    {
        GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();

        try (BuildDriver build = new BuildDriver(buildSideSupplier, buildTypes, buildOutputChannels, buildPages)) {
            assertThat(build.run()).isInstanceOf(Blocked.class);

            GpuCrossJoin.Factory factory = new GpuCrossJoin.Factory(buildSideSupplier, probeOutputChannels);
            List<Page> output;
            try {
                output = executeGpuOperation(probePages, probeTypes, outputTypes(probeTypes, probeOutputChannels, buildTypes, buildOutputChannels), factory::create);
            }
            finally {
                factory.noMoreOperators();
            }

            assertThat(build.run()).isInstanceOf(Finished.class);
            return output;
        }
    }

    private static long positionCount(List<Page> pages)
    {
        return pages.stream().mapToLong(Page::getPositionCount).sum();
    }

    private static Multiset<String> pairs(List<Page> pages)
    {
        return rows(pages, List.of(BIGINT, BIGINT));
    }

    private static Multiset<String> singleColumn(List<Page> pages)
    {
        return rows(pages, List.of(BIGINT));
    }

    private static Multiset<String> rows(List<Page> pages, List<Type> types)
    {
        ImmutableMultiset.Builder<String> rows = ImmutableMultiset.builder();
        for (Page page : pages) {
            assertThat(page.getChannelCount()).isEqualTo(types.size());
            for (int position = 0; position < page.getPositionCount(); position++) {
                StringJoiner row = new StringJoiner(":");
                for (int channel = 0; channel < types.size(); channel++) {
                    row.add(value(types.get(channel), page.getBlock(channel), position));
                }
                rows.add(row.toString());
            }
        }
        return rows.build();
    }

    private static String value(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return "null";
        }
        return switch (type) {
            case BigintType bigintType -> String.valueOf(bigintType.getLong(block, position));
            case VarcharType varcharType -> varcharType.getSlice(block, position).toStringUtf8();
            case VarbinaryType varbinaryType -> varbinaryType.getSlice(block, position).toStringUtf8();
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }

    private static Block longBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block nullableLongBlock(Long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private static Block varcharBlock(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }
        return builder.build();
    }

    private static Block varbinaryBlock(String... values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, values.length);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARBINARY.writeSlice(builder, utf8Slice(value));
            }
        }
        return builder.build();
    }

    private static Block sequenceBlock(int rows)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            BIGINT.writeLong(builder, i);
        }
        return builder.build();
    }

    /// Drives a [GpuCrossJoinBuild] pipeline. After buffering all input and publishing the bridge,
    /// the build operation reports [Blocked] until every probe operator has closed.
    private static final class BuildDriver
            implements AutoCloseable
    {
        private final BufferPages bufferPages;
        private final Iterator<Page> input;
        private final GpuOperation build;

        BuildDriver(GpuCrossJoinBuildSideSupplier buildSideSupplier, List<Type> buildTypes, int[] buildOutputChannels, List<Page> buildPages)
        {
            this.input = buildPages.iterator();
            GpuOperation.Context context = new TestingGpuOperationContext();
            this.bufferPages = new BufferPages(context.taskMemoryContext());
            Set<Integer> deviceChannels = IntStream.range(0, buildTypes.size()).boxed().collect(toImmutableSet());
            CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, buildTypes, deviceChannels);
            this.build = new GpuCrossJoinBuild.Factory(buildSideSupplier, buildOutputChannels).create(context, copyToDevice);
        }

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
}
