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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
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

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuCrossJoinMemory
{
    private static final int BUILD_ROWS = 500;
    private static final int PROBE_ROWS_PER_PAGE = 4_000;
    private static final int[] NO_CHANNELS = {};
    private static final int[] CHANNEL_0 = {0};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testBigint()
    {
        testProbe(List.of(BIGINT), CHANNEL_0, CHANNEL_0);
    }

    @Test
    void testVarchar()
    {
        testProbe(List.of(VARCHAR), CHANNEL_0, CHANNEL_0);
    }

    @Test
    void testProbeOutputOnly()
    {
        testProbe(List.of(BIGINT), CHANNEL_0, NO_CHANNELS);
    }

    @Test
    void testBuildOutputOnly()
    {
        testProbe(List.of(BIGINT), NO_CHANNELS, CHANNEL_0);
    }

    @Test
    void testNoOutputChannels()
    {
        testProbe(List.of(BIGINT), NO_CHANNELS, NO_CHANNELS);
    }

    /// Wide rows at the start of the build side, with a product small enough to fit one chunk. The
    /// size of the whole product is known exactly, unlike the size of a slice of it, so it is emitted
    /// as a single page and the reservation tracks it however the widths are spread across the rows.
    @Test
    void testWideBuildRowsAmongNarrowOnes()
    {
        int probeRows = 4;
        int buildRows = 100_000;
        int wideBuildRows = 10_000;
        int wideRowChars = 4_000;

        TestingGpuOperationContext context = new TestingGpuOperationContext();
        // Tight margin: the whole point is that the reservation tracks the chunk that is actually built
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
            GpuCrossJoinBuild.Factory buildFactory = new GpuCrossJoinBuild.Factory(buildSideSupplier, CHANNEL_0);

            try (var buildSource = new SettableGpuOperation();
                    GpuOperation build = buildFactory.create(context, buildSource)) {
                Page buildPage = new Page(wideRowsFirst(buildRows, wideBuildRows, wideRowChars));
                memoryValidation.withoutValidation(() -> driveBuild(context, build, buildSource, buildPage, List.of(VARCHAR)));

                GpuCrossJoin.Factory probeFactory = new GpuCrossJoin.Factory(buildSideSupplier, NO_CHANNELS);
                int chunks = 0;
                long outputRows = 0;
                try (var probeSource = new SettableGpuOperation();
                        GpuOperation probe = probeFactory.create(context, probeSource)) {
                    Page probePage = new Page(sequenceBlock(probeRows));
                    boolean probeFed = false;
                    boolean finished = false;
                    while (!finished) {
                        switch (probe.execute()) {
                            case Yielded() -> {
                                if (!probeSource.hasPending()) {
                                    boolean fed = probeFed;
                                    probeFed = true;
                                    memoryValidation.withoutValidation(() -> {
                                        if (fed) {
                                            probeSource.noMoreInput();
                                        }
                                        else {
                                            GpuPage next = getOnlyElement(copyToDevice(List.of(probePage), List.of(BIGINT)));
                                            probeSource.setPending(context.taskMemoryContext().allocate("probe", next.retainedMemory()), next);
                                        }
                                    });
                                }
                            }
                            case Data(AllocatedMemory memory, GpuPage page) -> {
                                chunks++;
                                outputRows += page.positionCount();
                                try (var closer = UncheckedCloser.create()) {
                                    closer.register(memory);
                                    closer.register(page);
                                }
                            }
                            case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                            case Finished() -> finished = true;
                        }
                    }
                }
                finally {
                    probeFactory.noMoreOperators();
                }

                assertThat(outputRows).isEqualTo((long) probeRows * buildRows);
                assertThat(chunks).isEqualTo(1);

                memoryValidation.withoutValidation(() -> drainBuild(build));
            }
        }
    }

    /// One probe row far wider than the rest, with a product too large for a single chunk. The chunk
    /// row count follows the widest row rather than the page average, which on its own would leave the
    /// chunk at the row cap. Reservations are not validated: a chunk holding the one wide row among
    /// thousands of narrow ones is far smaller than a width-derived reservation for it.
    @Test
    void testChunkRowsBoundedByWidestRow()
    {
        int probeRows = 250_000;
        int wideRowChars = 4_000;
        int buildRows = 100;

        TestingGpuOperationContext context = new TestingGpuOperationContext();
        GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
        GpuCrossJoinBuild.Factory buildFactory = new GpuCrossJoinBuild.Factory(buildSideSupplier, CHANNEL_0);

        try (var buildSource = new SettableGpuOperation();
                GpuOperation build = buildFactory.create(context, buildSource)) {
            driveBuild(context, build, buildSource, new Page(sequenceBlock(buildRows)), List.of(BIGINT));

            GpuCrossJoin.Factory probeFactory = new GpuCrossJoin.Factory(buildSideSupplier, CHANNEL_0);
            int firstChunkRows = 0;
            try (var probeSource = new SettableGpuOperation();
                    GpuOperation probe = probeFactory.create(context, probeSource)) {
                Page probePage = new Page(wideRowsFirst(probeRows, 1, wideRowChars));
                boolean probeFed = false;
                while (firstChunkRows == 0) {
                    switch (probe.execute()) {
                        case Yielded() -> {
                            if (!probeSource.hasPending() && !probeFed) {
                                probeFed = true;
                                GpuPage next = getOnlyElement(copyToDevice(List.of(probePage), List.of(VARCHAR)));
                                probeSource.setPending(context.taskMemoryContext().allocate("probe", next.retainedMemory()), next);
                            }
                        }
                        case Data(AllocatedMemory memory, GpuPage page) -> {
                            firstChunkRows = page.positionCount();
                            try (var closer = UncheckedCloser.create()) {
                                closer.register(memory);
                                closer.register(page);
                            }
                        }
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                        case Finished() -> throw new IllegalStateException("Unexpected finish");
                    }
                }
            }
            finally {
                probeFactory.noMoreOperators();
            }

            assertThat(firstChunkRows).isLessThan(1 << 20);

            drainBuild(build);
        }
    }

    /// A probe operator that closes while the build side is still blocked on the other probes: the
    /// build side must stay alive, and its memory must be released only once the last one is gone.
    @Test
    void testProbeClosedWhileBuildIsBlocked()
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
            GpuCrossJoinBuild.Factory buildFactory = new GpuCrossJoinBuild.Factory(buildSideSupplier, CHANNEL_0);

            try (var buildSource = new SettableGpuOperation();
                    GpuOperation build = buildFactory.create(context, buildSource)) {
                Page buildPage = new Page(wideRowsFirst(BUILD_ROWS, 1, 8));
                memoryValidation.withoutValidation(() -> driveBuild(context, build, buildSource, buildPage, List.of(VARCHAR)));
                long publishedBytes = context.taskMemoryContext().taskGpuDeviceMemory().getBytes();
                assertThat(publishedBytes).isGreaterThan(0);

                GpuCrossJoin.Factory probeFactory = new GpuCrossJoin.Factory(buildSideSupplier, NO_CHANNELS);
                GpuCrossJoin.Factory duplicatedProbeFactory = probeFactory.duplicate();
                // A probe operator that closes without ever consuming a page
                try (GpuOperation probe = probeFactory.create(context, new SettableGpuOperation())) {
                    assertThat(probe).isNotNull();
                }
                probeFactory.noMoreOperators();

                // The build side outlives that probe: the second factory still holds it
                assertThat(build.execute()).isInstanceOf(Blocked.class);
                assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes()).isEqualTo(publishedBytes);

                duplicatedProbeFactory.noMoreOperators();
                memoryValidation.withoutValidation(() -> drainBuild(build));
            }
            assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes()).isEqualTo(0);
        }
    }

    private static void driveBuild(TestingGpuOperationContext context, GpuOperation build, SettableGpuOperation buildSource, Page buildPage, List<Type> types)
    {
        boolean fed = false;
        boolean blocked = false;
        while (!blocked) {
            switch (build.execute()) {
                case Yielded() -> {
                    if (!buildSource.hasPending()) {
                        if (fed) {
                            buildSource.noMoreInput();
                        }
                        else {
                            fed = true;
                            GpuPage next = getOnlyElement(copyToDevice(List.of(buildPage), types));
                            buildSource.setPending(context.taskMemoryContext().allocate("build", next.retainedMemory()), next);
                        }
                    }
                }
                case Blocked _ -> blocked = true;
                case Data _ -> throw new IllegalStateException("Build should not emit data");
                case Finished() -> throw new IllegalStateException("Unexpected finish");
            }
        }
    }

    private static void drainBuild(GpuOperation build)
    {
        while (true) {
            switch (build.execute()) {
                case Yielded() -> {}
                case Data _ -> throw new IllegalStateException("Build should not emit data");
                case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                case Finished() -> {
                    return;
                }
            }
        }
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

    private static Block sequenceBlock(int rows)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            BIGINT.writeLong(builder, i);
        }
        return builder.build();
    }

    private void testProbe(List<Type> types, int[] probeOutputChannels, int[] buildOutputChannels)
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            testProbe(types, nullsProvider, probeOutputChannels, buildOutputChannels);
        }
    }

    private void testProbe(List<Type> types, NullsProvider nullsProvider, int[] probeOutputChannels, int[] buildOutputChannels)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        // Larger margin because the allowance for cuDF's variable-width gather scratch is an upper bound that some inputs do not reach.
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 9L * 1024 * 1024)) {
            GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
            GpuCrossJoinBuild.Factory buildFactory = new GpuCrossJoinBuild.Factory(buildSideSupplier, buildOutputChannels);

            try (var buildSource = new SettableGpuOperation();
                    GpuOperation build = buildFactory.create(context, buildSource)) {
                // Drive build to published — exempt from validation since we're testing probe
                memoryValidation.withoutValidation(() -> {
                    Iterator<Page> buildPages = createPages(types, nullsProvider, 1, BUILD_ROWS, false);
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

                Iterator<Page> probePages = createPages(types, nullsProvider, 1, PROBE_ROWS_PER_PAGE, false);
                GpuCrossJoin.Factory probeFactory = new GpuCrossJoin.Factory(buildSideSupplier, probeOutputChannels);
                long outputRows = 0;
                try (var probeSource = new SettableGpuOperation();
                        GpuOperation probe = probeFactory.create(context, probeSource)) {
                    boolean finished = false;
                    while (!finished) {
                        switch (probe.execute()) {
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
                                outputRows += page.positionCount();
                                try (var closer = UncheckedCloser.create()) {
                                    closer.register(memory);
                                    closer.register(page);
                                }
                            }
                            case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                            case Finished() -> finished = true;
                        }
                    }
                }
                finally {
                    probeFactory.noMoreOperators();
                }
                assertThat(outputRows).isEqualTo((long) PROBE_ROWS_PER_PAGE * BUILD_ROWS);

                memoryValidation.withoutValidation(() -> {
                    assertThat(build.execute()).isInstanceOf(Finished.class);
                });
            }
        }
    }
}
