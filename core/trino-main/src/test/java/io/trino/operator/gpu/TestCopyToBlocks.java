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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.ColumnVector;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlocks;
import static io.trino.operator.gpu.GpuTestUtils.drainToPages;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.RANDOM_NULLS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class TestCopyToBlocks
{
    @Test
    void testCopiesPreExistingBlocks()
    {
        testCopiesPreExistingBlocks(createBlocks(List.of(0), RANDOM_NULLS, INTEGER));
        testCopiesPreExistingBlocks(createBlocks(List.of(1024), RANDOM_NULLS, INTEGER));
        testCopiesPreExistingBlocks(createBlocks(List.of(0, 17, 0, 1, 1, 1, 45, 757, 0, 3), RANDOM_NULLS, INTEGER));
        testCopiesPreExistingBlocks(List.of(RunLengthEncodedBlock.create(INTEGER, 42L, 1024)));
        testCopiesPreExistingBlocks(
                ImmutableList.<Block>builder()
                        .addAll(createBlocks(List.of(1024), RANDOM_NULLS, INTEGER))
                        .add(RunLengthEncodedBlock.create(INTEGER, 42L, 1024))
                        .addAll(createBlocks(List.of(1024), RANDOM_NULLS, INTEGER))
                        .add(RunLengthEncodedBlock.create(INTEGER, 42L, 1024))
                        .addAll(createBlocks(List.of(1024), RANDOM_NULLS, INTEGER))
                        .build());
    }

    private static void testCopiesPreExistingBlocks(List<Block> inputBlocks)
    {
        GpuPage gpuPage;
        try (Blocks blocks = new Blocks(inputBlocks)) {
            gpuPage = new GpuPage(blocks.positionCount(), new Column[] {blocks});
        }

        GpuOperation.Context context = new TestingGpuOperationContext();
        try (GpuOperation sourceOperation = singlePageSource(context, gpuPage);
                CopyToBlocks operation = new CopyToBlocks(context, sourceOperation, ImmutableList.of(INTEGER))) {
            List<Page> output = drainToPages(() -> {}, operation);
            assertSameDataInOrder(
                    output,
                    inputBlocks.stream()
                            .map(Page::new)
                            .toList(),
                    List.of(INTEGER));
        }
    }

    @Test
    void testCopiesNullWithUnsetValue()
    {
        // A null position built from a null scalar, as GpuGroupId does for the keys a grouping set leaves
        // out, may have an uninitialized slot.
        TimestampType type = createTimestampType(3);
        GpuPage gpuPage;
        try (ColumnVector values = ColumnVector.timestampMilliSecondsFromLongs(Long.MAX_VALUE, 1);
                ColumnVector validity = ColumnVector.fromBoxedBooleans(null, true);
                Column column = new DeviceMemory(values.mergeAndSetValidity(BinaryOp.BITWISE_AND, validity))) {
            gpuPage = new GpuPage(2, new Column[] {column});
        }

        GpuOperation.Context context = new TestingGpuOperationContext();
        try (GpuOperation sourceOperation = singlePageSource(context, gpuPage);
                CopyToBlocks operation = new CopyToBlocks(context, sourceOperation, ImmutableList.of(type))) {
            Page output = getOnlyElement(drainToPages(() -> {}, operation));
            assertThat(output.getPositionCount()).isEqualTo(2);
            assertThat(output.getBlock(0).isNull(0)).isTrue();
            assertThat(type.getLong(output.getBlock(0), 1)).isEqualTo(1_000); // 1 millisecond in Trino's microseconds
        }
    }

    private static GpuOperation singlePageSource(GpuOperation.Context context, GpuPage page)
    {
        requireNonNull(page, "page is null");
        return new GpuOperation()
        {
            private GpuPage pending = page;

            @Override
            public Result execute()
            {
                if (pending == null) {
                    return new Finished();
                }
                GpuPage next = pending;
                pending = null;
                AllocatedMemory allocated = context.taskMemoryContext().allocate(getClass().getSimpleName(), page.retainedMemory());
                return new Data(allocated, next);
            }

            @Override
            public void close()
            {
                if (pending != null) {
                    pending.close();
                    pending = null;
                }
            }
        };
    }
}
