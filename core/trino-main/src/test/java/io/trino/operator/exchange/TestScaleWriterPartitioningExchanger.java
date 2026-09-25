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
package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.operator.output.SkewedPartitionRebalancer;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.sql.planner.MergePartitioningHandle.MergePartitionFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

class TestScaleWriterPartitioningExchanger
{
    private static final long MAX_BYTES = DataSize.of(1, GIGABYTE).toBytes();

    @Test
    void testMergeDeletedRowsStayOnFirstWriter()
    {
        // One partition and three writers; two rebalance cycles over the threshold assign all three writers to the partition
        SkewedPartitionRebalancer rebalancer = new SkewedPartitionRebalancer(1, 3, 1, DataSize.of(1, MEGABYTE).toBytes(), DataSize.of(50, MEGABYTE).toBytes());
        for (int cycle = 0; cycle < 2; cycle++) {
            rebalancer.addPartitionRowCount(0, 1000);
            rebalancer.addDataProcessed(DataSize.of(60, MEGABYTE).toBytes());
            rebalancer.rebalance();
        }

        // Each writer records the row values it receives
        List<List<Long>> writerValues = ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        List<Consumer<Page>> buffers = writerValues.stream()
                .map(values -> (Consumer<Page>) page -> {
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        values.add(BIGINT.getLong(page.getBlock(1), position));
                    }
                })
                .collect(toImmutableList());
        // Channel 0 holds the merge operation and channel 1 the row value; every row maps to partition 0
        PartitionFunction singlePartition = new BucketPartitionFunction((_, _) -> 0, new int[] {0});
        ScaleWriterPartitioningExchanger exchanger = new ScaleWriterPartitioningExchanger(
                buffers,
                new LocalExchangeMemoryManager(MAX_BYTES),
                MAX_BYTES,
                identity(),
                new MergePartitionFunction(singlePartition, singlePartition, new int[] {}, new int[] {1}),
                1,
                rebalancer,
                () -> 0L,
                MAX_BYTES,
                100);

        // Three pages of values 0 to 11 that alternate between inserted (even) and deleted (odd) rows
        byte[] operations = {INSERT_OPERATION_NUMBER, DELETE_OPERATION_NUMBER, INSERT_OPERATION_NUMBER, DELETE_OPERATION_NUMBER};
        for (int page = 0; page < 3; page++) {
            exchanger.accept(new Page(
                    new ByteArrayBlock(operations.length, Optional.empty(), operations),
                    createSequencePage(ImmutableList.of(BIGINT), operations.length, page * operations.length).getBlock(0)));
        }

        // Deleted rows always land on the first writer, while each page of inserted rows moves to the next writer
        assertThat(writerValues.getFirst()).containsExactly(0L, 1L, 2L, 3L, 5L, 7L, 9L, 11L);
        assertThat(writerValues.subList(1, 3)).containsExactlyInAnyOrder(ImmutableList.of(4L, 6L), ImmutableList.of(8L, 10L));
    }
}
