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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

class TestMergePartitioningHandle
{
    private static final int[] BUCKET_TO_PARTITION = {0, 1, 2, 3};

    @Test
    void testUpdateLayoutWithoutInsertLayout()
    {
        assertThat(getPartitions(createUpdateOnlyPartitionFunction(false), createMergePage())).containsExactly(0, 3, 1, 1, 2);
    }

    @Test
    void testScaledUpdateLayoutWithoutInsertLayout()
    {
        assertThat(getPartitions(createUpdateOnlyPartitionFunction(true), createMergePage())).containsExactly(0, 3, 0, 1, 0);
    }

    private static PartitionFunction createUpdateOnlyPartitionFunction(boolean scaleWriters)
    {
        Symbol rowId = new Symbol(BIGINT, "row_id");
        MergePartitioningHandle handle = new MergePartitioningHandle(
                Optional.empty(),
                Optional.of(new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(rowId)), ImmutableList.of(rowId))));
        PartitionFunction updateFunction = new BucketPartitionFunction((page, position) -> toIntExact(BIGINT.getLong(page.getBlock(0), position) % 4), BUCKET_TO_PARTITION);
        return handle.getPartitionFunction((_, _) -> updateFunction, ImmutableList.of(TINYINT, BIGINT), BUCKET_TO_PARTITION, scaleWriters);
    }

    private static Page createMergePage()
    {
        byte[] operations = {INSERT_OPERATION_NUMBER, DELETE_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER, INSERT_OPERATION_NUMBER};
        return new Page(
                new ByteArrayBlock(operations.length, Optional.empty(), operations),
                createLongsBlock(null, 3L, 5L, 5L, null));
    }

    private static int[] getPartitions(PartitionFunction partitionFunction, Page page)
    {
        int[] partitions = new int[page.getPositionCount()];
        partitionFunction.getPartitions(page, 0, page.getPositionCount(), partitions);
        return partitions;
    }
}
