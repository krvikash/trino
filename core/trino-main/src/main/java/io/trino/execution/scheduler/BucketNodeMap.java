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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.trino.exchange.ExchangeInput;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Objects.requireNonNull;

public final class BucketNodeMap
{
    private final List<InternalNode> bucketToNode;
    private final Optional<List<InternalNode>> partitionToNode;
    private final ToIntFunction<Split> splitToBucket;

    public BucketNodeMap(ToIntFunction<Split> splitToBucket, List<InternalNode> bucketToNode)
    {
        this(splitToBucket, bucketToNode, Optional.empty());
    }

    public BucketNodeMap(ToIntFunction<Split> splitToBucket, List<InternalNode> bucketToNode, Optional<List<InternalNode>> partitionToNode)
    {
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
        this.bucketToNode = ImmutableList.copyOf(requireNonNull(bucketToNode, "bucketToNode is null"));
        this.partitionToNode = partitionToNode.map(ImmutableList::copyOf);
    }

    public int getBucketCount()
    {
        return bucketToNode.size();
    }

    public int getBucket(Split split)
    {
        return splitToBucket.applyAsInt(split);
    }

    public InternalNode getAssignedNode(int bucketId)
    {
        return bucketToNode.get(bucketId);
    }

    public InternalNode getAssignedNode(Split split)
    {
        if (split.getConnectorSplit() instanceof RemoteSplit remoteSplit) {
            checkState(partitionToNode.isPresent(), "partitionToNode must be set to handle RemoteSplit");
            ExchangeInput exchangeInput = remoteSplit.getExchangeInput();
            checkArgument(exchangeInput instanceof SpoolingExchangeInput, "Expected SpoolingExchangeInput in RemoteSplit");
            SpoolingExchangeInput spoolingExchangeInput = (SpoolingExchangeInput) exchangeInput;
            List<ExchangeSourceHandle> handles = spoolingExchangeInput.getExchangeSourceHandles();
            Set<Integer> partitionIds = handles.stream().map(ExchangeSourceHandle::getPartitionId).collect(toImmutableSet());
            checkArgument(partitionIds.size() == 1, "RemoteSplit referencing more than one partition: %s", partitionIds);
            return partitionToNode.get().get(getFirst(partitionIds, null));
        }
        return getAssignedNode(getBucket(split));
    }

    public ToIntFunction<Split> getSplitToBucketFunction()
    {
        return splitToBucket;
    }
}
