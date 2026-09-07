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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuCrossJoinBuildMemory
{
    private static final int ROWS_PER_PAGE = 100_000;
    private static final int MULTIPLE_PAGES = 4;
    private static final int[] NO_CHANNELS = {};
    private static final int[] CHANNEL_0 = {0};

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testBigintOutputChannel()
    {
        testBuild(List.of(BIGINT), CHANNEL_0, 1);
        testBuild(List.of(BIGINT), CHANNEL_0, MULTIPLE_PAGES);
    }

    @Test
    void testVarcharOutputChannel()
    {
        testBuild(List.of(VARCHAR), CHANNEL_0, 1);
        testBuild(List.of(VARCHAR), CHANNEL_0, MULTIPLE_PAGES);
    }

    /// Only one of two channels is selected, so the reservation has to shrink to that channel once
    /// the input page is released.
    @Test
    void testUnselectedChannelsAreNotRetained()
    {
        testBuild(List.of(BIGINT, VARCHAR), CHANNEL_0, MULTIPLE_PAGES);
    }

    /// `COUNT(*)`-shaped build: nothing is buffered, so the build must hold no device memory at all.
    @Test
    void testCountOnlyBuildRetainsNothing()
    {
        testBuild(List.of(BIGINT, VARCHAR), NO_CHANNELS, MULTIPLE_PAGES);
    }

    /// Closed while pages are still buffered, before the build side is published. Everything
    /// buffered has to be released, and the future the operator would block on is completed
    /// exceptionally instead of being left pending.
    @Test
    void testClosedBeforePublishing()
    {
        List<Type> types = List.of(VARCHAR);
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(types, NO_NULLS, 1, ROWS_PER_PAGE, true);

            GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
            GpuCrossJoinBuild.Factory factory = new GpuCrossJoinBuild.Factory(buildSideSupplier, CHANNEL_0);
            try (var source = new SettableGpuOperation();
                    GpuOperation build = factory.create(context, source)) {
                memoryValidation.withoutValidation(() -> {
                    GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), types));
                    source.setPending(context.taskMemoryContext().allocate("input", next.retainedMemory()), next);
                });
                // Buffers the page, and stops short of publishing because the input is not exhausted
                assertThat(build.execute()).isInstanceOf(Yielded.class);
                assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes()).isGreaterThan(0);
            }

            assertThat(buildSideSupplier.getBuildSideFuture()).isNotDone();
            assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes()).isEqualTo(0);
        }
    }

    private void testBuild(List<Type> types, int[] buildOutputChannels, int pageCount)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(types, NO_NULLS, pageCount, ROWS_PER_PAGE, true);
            long[] inputBytes = new long[2]; // all channels, selected channels

            GpuCrossJoinBuildSideSupplier buildSideSupplier = new GpuCrossJoinBuildSideSupplier();
            GpuCrossJoinBuild.Factory factory = new GpuCrossJoinBuild.Factory(buildSideSupplier, buildOutputChannels);
            try (var source = new SettableGpuOperation();
                    GpuOperation build = factory.create(context, source)) {
                ListenableFuture<Void> blockedFuture = null;
                while (blockedFuture == null) {
                    switch (build.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                if (inputPages.hasNext()) {
                                    Page page = inputPages.next();
                                    memoryValidation.withoutValidation(() -> {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(page), types));
                                        inputBytes[0] += channelBytes(next, allChannels(types));
                                        inputBytes[1] += channelBytes(next, buildOutputChannels);
                                        source.setPending(context.taskMemoryContext().allocate("input", next.retainedMemory()), next);
                                    });
                                }
                                else {
                                    source.noMoreInput();
                                }
                            }
                        }
                        case Blocked(var future) -> {
                            checkState(source.isExhausted(), "Input not exhausted yet");
                            blockedFuture = future;
                        }
                        case Data _ -> throw new IllegalStateException("Build should not emit data");
                        case Finished() -> throw new IllegalStateException("Unexpected finish");
                    }
                }

                // Published: the build side owns everything still reserved, the inputs are gone.
                assertRetainedAfterPublish(context, buildOutputChannels, types, inputBytes[0], inputBytes[1]);

                verify(!blockedFuture.isDone(), "blockedFuture done too early");
                buildSideSupplier.probeOperatorFactoryClosed();
                verify(blockedFuture.isDone(), "blockedFuture still not done");

                boolean finished = false;
                while (!finished) {
                    switch (build.execute()) {
                        case Yielded() -> {}
                        case Data _ -> throw new IllegalStateException("Build should not emit data");
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }

            assertThat(context.taskMemoryContext().taskGpuDeviceMemory().getBytes()).isEqualTo(0);
        }
    }

    private static void assertRetainedAfterPublish(
            TestingGpuOperationContext context,
            int[] buildOutputChannels,
            List<Type> types,
            long allChannelBytes,
            long selectedChannelBytes)
    {
        long retained = context.taskMemoryContext().taskGpuDeviceMemory().getBytes();
        if (buildOutputChannels.length == 0) {
            assertThat(retained).isEqualTo(0);
            return;
        }
        assertThat(retained).isLessThanOrEqualTo(allChannelBytes);
        if (buildOutputChannels.length < types.size()) {
            assertThat(retained).isLessThan(selectedChannelBytes + (allChannelBytes - selectedChannelBytes) / 2);
        }
        // Concatenating per-page columns merges their offset arrays, so the result is allowed to be
        // slightly smaller than the pages were.
        assertThat(retained).isGreaterThan(selectedChannelBytes * 9 / 10);
    }

    private static int[] allChannels(List<Type> types)
    {
        int[] channels = new int[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = i;
        }
        return channels;
    }

    private static long channelBytes(GpuPage page, int[] channels)
    {
        long bytes = 0;
        for (int channel : channels) {
            bytes += page.column(channel).retainedDeviceMemoryBytes();
        }
        return bytes;
    }
}
