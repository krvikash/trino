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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.trino.execution.RemoteTask;
import io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

/**
 * Runs a single-node query on the node it reserves from {@link SingleNodeQueryPlacements}.
 */
public class SingleNodeSelector
        implements NodeSelector
{
    private final NodeSelector delegate;
    private final Supplier<InternalNode> node;

    public SingleNodeSelector(NodeSelector delegate, QueryId queryId, SingleNodeQueryPlacements placements, Supplier<Phase> phase)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(placements, "placements is null");
        requireNonNull(phase, "phase is null");
        this.node = Suppliers.memoize(() -> placements.reserveLeastLoaded(queryId, delegate.allNodes(), phase)
                .orElseThrow(() -> new TrinoException(NO_NODES_AVAILABLE, "No worker nodes available")));
    }

    @Override
    public void lockDownNodes()
    {
        delegate.lockDownNodes();
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return ImmutableList.of(node.get());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        return delegate.selectCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        InternalNode node = this.node.get();
        if (limit <= 0 || excludedNodes.contains(node)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(node);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        // every stage scheduled with this selector is bucketed onto the query's node
        throw new UnsupportedOperationException("A single-node query places its splits through a bucket node map");
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        verify(bucketNodeMap.getDistinctNodes().equals(allNodes()), "Bucket node map on %s would run the query on a node it did not reserve", bucketNodeMap.getDistinctNodes());
        return delegate.computeAssignments(splits, existingTasks, bucketNodeMap);
    }
}
