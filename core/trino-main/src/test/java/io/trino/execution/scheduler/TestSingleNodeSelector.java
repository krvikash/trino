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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.DefaultNodeManager;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase.RUNNING;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSingleNodeSelector
{
    private static final InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
    private static final InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);

    private final Map<QueryId, Phase> phases = new HashMap<>();
    private final SingleNodeQueryPlacements placements = new SingleNodeQueryPlacements();
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private TestingInternalNodeManager nodeManager;
    private NodeScheduler nodeScheduler;

    @BeforeEach
    void setUp()
    {
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = TestingInternalNodeManager.createDefault();
        nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                CURRENT_NODE,
                nodeManager,
                new NodeSchedulerConfig().setIncludeCoordinator(false),
                nodeTaskMap,
                new StableHostAddressProvider(new DefaultNodeManager(CURRENT_NODE, nodeManager, false), new StableHostAddressProviderConfig()),
                // see every node change at once
                new Duration(0, SECONDS)));
        finalizerService.start();
    }

    @AfterEach
    void tearDown()
    {
        finalizerService.destroy();
        finalizerService = null;
    }

    @Test
    void testEveryPathLandsOnReservedNode()
    {
        nodeManager.addNodes(node1, node2);
        NodeSelector selector = createSingleNodeSelector("query");
        InternalNode node = getOnlyElement(selector.allNodes());
        InternalNode otherNode = node.equals(node1) ? node2 : node1;

        // every path of the selector
        assertThat(selector.allNodes()).containsExactly(node);
        assertThat(selector.selectRandomNodes(2)).containsExactly(node);
        assertThat(selector.selectRandomNodes(1, ImmutableSet.of(node))).isEmpty();
        assertThat(selector.selectCurrentNode()).isEqualTo(CURRENT_NODE);

        // the stage is bucketed onto its node, so split addresses play no part
        Split anywhere = split(true, ImmutableList.of());
        Split onOtherNode = split(false, ImmutableList.of(otherNode.getHostAndPort()));
        Set<Split> splits = ImmutableSet.of(anywhere, onOtherNode);
        SplitPlacementResult placement = selector.computeAssignments(splits, ImmutableList.of(), new BucketNodeMap(_ -> 0, ImmutableList.of(node)));
        assertThat(placement.getAssignments().keySet()).containsExactly(node);
        assertThat(placement.getAssignments().get(node)).containsExactlyInAnyOrder(anywhere, onOtherNode);

        assertThatThrownBy(() -> selector.computeAssignments(splits, ImmutableList.of(), new BucketNodeMap(_ -> 0, ImmutableList.of(otherNode))))
                .isInstanceOf(VerifyException.class);
        assertThatThrownBy(() -> selector.computeAssignments(splits, ImmutableList.of()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testNoWorkers()
    {
        NodeSelector selector = createSingleNodeSelector("query");
        assertThatThrownBy(selector::allNodes)
                .isInstanceOf(TrinoException.class)
                .hasMessage("No worker nodes available");

        // a worker joins after the failed reservation
        nodeManager.addNodes(node1);
        assertThat(selector.allNodes()).containsExactly(node1);
    }

    @Test
    void testRetriedQueryMovesWhenItsNodeLeaves()
    {
        nodeManager.addNodes(node1);
        assertThat(createSingleNodeSelector("query").allNodes()).containsExactly(node1);

        // the node leaves between two attempts, each with its own selector
        nodeManager.removeNode(node1);
        nodeManager.addNodes(node2);
        assertThat(createSingleNodeSelector("query").allNodes()).containsExactly(node2);
    }

    private NodeSelector createSingleNodeSelector(String queryId)
    {
        QueryId id = new QueryId(queryId);
        Session session = TestingSession.testSessionBuilder()
                .setQueryId(id)
                .build();
        return new SingleNodeSelector(nodeScheduler.createNodeSelector(session), id, placements, () -> phases.getOrDefault(id, RUNNING));
    }

    private static Split split(boolean remotelyAccessible, List<HostAddress> addresses)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingSplit(remotelyAccessible, addresses));
    }
}
