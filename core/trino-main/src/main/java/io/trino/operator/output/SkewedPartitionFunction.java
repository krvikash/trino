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

package io.trino.operator.output;

import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.sql.planner.MergePartitioningHandle.MergePartitionFunction;

import static java.util.Objects.requireNonNull;

public class SkewedPartitionFunction
        implements PartitionFunction
{
    private final PartitionFunction partitionFunction;
    private final SkewedPartitionRebalancer skewedPartitionRebalancer;

    private final long[] partitionRowCount;
    private final long[] pinnedPartitionRowCount;

    public SkewedPartitionFunction(PartitionFunction partitionFunction, SkewedPartitionRebalancer skewedPartitionRebalancer)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.skewedPartitionRebalancer = requireNonNull(skewedPartitionRebalancer, "skewedPartitionRebalancer is null");

        this.partitionRowCount = new long[partitionFunction.partitionCount()];
        this.pinnedPartitionRowCount = new long[partitionFunction.partitionCount()];
    }

    @Override
    public int partitionCount()
    {
        return skewedPartitionRebalancer.getTaskCount();
    }

    @Override
    public int getPartition(Page page, int position)
    {
        int partition = partitionFunction.getPartition(page, position);
        // Deleted rows of a data file stay on the first task of their partition
        if (partitionFunction instanceof MergePartitionFunction mergePartitionFunction && mergePartitionFunction.isDeletedRow(page, position)) {
            pinnedPartitionRowCount[partition]++;
            return skewedPartitionRebalancer.getTaskId(partition, 0);
        }
        return skewedPartitionRebalancer.getTaskId(partition, partitionRowCount[partition]++);
    }

    public void flushPartitionRowCountToRebalancer()
    {
        for (int partition = 0; partition < partitionFunction.partitionCount(); partition++) {
            skewedPartitionRebalancer.addPartitionRowCount(partition, partitionRowCount[partition] + pinnedPartitionRowCount[partition]);
            partitionRowCount[partition] = 0;
            pinnedPartitionRowCount[partition] = 0;
        }
    }
}
