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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase;
import io.trino.node.InternalNode;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase.COMPLETED;
import static io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase.RUNNING;
import static io.trino.execution.scheduler.SingleNodeQueryPlacements.Phase.TEARING_DOWN;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSingleNodeQueryPlacements
{
    private static final InternalNode NODE_A = new InternalNode("nodeA", URI.create("http://10.0.0.1:1"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_B = new InternalNode("nodeB", URI.create("http://10.0.0.1:2"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_C = new InternalNode("nodeC", URI.create("http://10.0.0.1:3"), NodeVersion.UNKNOWN, false);
    private static final List<InternalNode> ALL_NODES = ImmutableList.of(NODE_A, NODE_B, NODE_C);

    private final AtomicInteger nextQueryId = new AtomicInteger();
    // A query without an entry is running
    private final Map<QueryId, Phase> phases = new ConcurrentHashMap<>();
    private SingleNodeQueryPlacements placements;
    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        placements = new SingleNodeQueryPlacements();
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void reservationsSpreadAcrossIdleNodes()
    {
        // Three queries arrive at three idle nodes
        List<InternalNode> chosen = ImmutableList.of(reserve(ALL_NODES), reserve(ALL_NODES), reserve(ALL_NODES));

        assertThat(chosen).containsExactly(NODE_A, NODE_B, NODE_C);
    }

    @Test
    void concurrentReservationsSpreadEvenly()
            throws Exception
    {
        // Many threads reserve at once against the same three nodes
        int reservationsPerNode = 50;
        int threads = reservationsPerNode * ALL_NODES.size();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<InternalNode>> futures = IntStream.range(0, threads)
                .mapToObj(_ -> executor.submit((Callable<InternalNode>) () -> {
                    start.await();
                    return reserve(ALL_NODES);
                }))
                .collect(toImmutableList());
        start.countDown();

        ImmutableMultiset.Builder<InternalNode> chosen = ImmutableMultiset.builder();
        for (Future<InternalNode> future : futures) {
            chosen.add(future.get());
        }
        Multiset<InternalNode> counts = chosen.build();
        for (InternalNode node : ALL_NODES) {
            assertThat(counts.count(node)).isEqualTo(reservationsPerNode);
        }
    }

    @Test
    void concurrentQueriesNeverShareANode()
            throws Exception
    {
        // Each thread places its next query once the previous one completes, so a node without a running query always exists
        int queriesPerThread = 2_000;
        Map<InternalNode, QueryId> occupied = new ConcurrentHashMap<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = ALL_NODES.stream()
                .map(_ -> executor.submit(() -> {
                    start.await();
                    for (int query = 0; query < queriesPerThread; query++) {
                        QueryId queryId = nextQueryId();
                        InternalNode node = placements.reserveLeastLoaded(queryId, ALL_NODES, phase(queryId)).orElseThrow();
                        // A holder means two running queries share the node
                        QueryId holder = occupied.putIfAbsent(node, queryId);
                        assertThat(holder)
                                .describedAs("%s placed on %s, which %s still holds", queryId, node, holder)
                                .isNull();
                        // Clear before completing so the next query on the node does not see this one
                        occupied.remove(node, queryId);
                        phases.put(queryId, COMPLETED);
                    }
                    return null;
                }))
                .collect(toImmutableList());
        start.countDown();

        for (Future<?> future : futures) {
            future.get(60, SECONDS);
        }
    }

    @Test
    void completedQueryFreesItsNode()
    {
        QueryId queryOnA = reserveOn(NODE_A);
        reserveOn(NODE_B);

        phases.put(queryOnA, COMPLETED);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_A);
    }

    @Test
    void tearingDownNodeBeatsRunningNode()
    {
        reserveOn(NODE_A);
        QueryId queryOnB = reserveOn(NODE_B);

        phases.put(queryOnB, TEARING_DOWN);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_B);
    }

    @Test
    void idleNodesBeatTearingDownNode()
    {
        QueryId queryOnA = reserveOn(NODE_A);
        phases.put(queryOnA, TEARING_DOWN);

        assertThat(reserve(ALL_NODES)).isEqualTo(NODE_B);
        assertThat(reserve(ALL_NODES)).isEqualTo(NODE_C);
        // Every other node runs a query
        assertThat(reserve(ALL_NODES)).isEqualTo(NODE_A);
    }

    @Test
    void fewerTearingDownQueriesWin()
    {
        QueryId firstOnA = reserveOn(NODE_A);
        QueryId secondOnA = reserveOn(NODE_A);
        QueryId queryOnB = reserveOn(NODE_B);
        phases.put(firstOnA, TEARING_DOWN);
        phases.put(secondOnA, TEARING_DOWN);
        phases.put(queryOnB, TEARING_DOWN);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_B);
    }

    @Test
    void releaseFreesNode()
    {
        // A released query's node is free even though its phase still reads running
        reserveOn(NODE_B);
        AtomicInteger asked = new AtomicInteger();
        QueryId queryId = nextQueryId();
        Supplier<Phase> running = () -> {
            asked.incrementAndGet();
            return RUNNING;
        };
        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(NODE_A), running)).contains(NODE_A);

        placements.release(queryId);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_A);
        assertThat(asked).hasValue(0);
    }

    @Test
    void queryKeepsItsNodeWhenReservedAgain()
    {
        // A retried query keeps its node even when another node is less loaded
        QueryId queryId = reserveOn(NODE_A);
        reserveOn(NODE_A);
        reserveOn(NODE_B);

        assertThat(placements.reserveLeastLoaded(queryId, ALL_NODES, phase(queryId))).contains(NODE_A);
    }

    @Test
    void queryWithoutCandidatesKeepsTearingDownOnItsNode()
    {
        // A retry attempt that finds no node fails the query, whose first attempt's node then counts as tearing down
        QueryId queryId = reserveOn(NODE_A);

        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(), phase(queryId))).isEmpty();
        phases.put(queryId, TEARING_DOWN);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_B);
        phases.put(queryId, COMPLETED);
        assertThat(reserve(ImmutableList.of(NODE_A, NODE_B))).isEqualTo(NODE_A);
    }

    @Test
    void movedQueryTearsDownOnItsFormerNode()
    {
        QueryId queryId = reserveOn(NODE_A);
        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(NODE_B, NODE_C), phase(queryId))).contains(NODE_B);

        // The moved query may still have tasks on A
        assertThat(reserve(ALL_NODES)).isEqualTo(NODE_C);
        assertThat(reserve(ALL_NODES)).isEqualTo(NODE_A);
    }

    @Test
    void completedQueryFreesItsFormerNode()
    {
        QueryId queryId = reserveOn(NODE_A);
        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(NODE_B), phase(queryId))).contains(NODE_B);
        assertThat(reserve(ImmutableList.of(NODE_A, NODE_C))).isEqualTo(NODE_C);

        phases.put(queryId, COMPLETED);

        assertThat(reserve(ImmutableList.of(NODE_A, NODE_C))).isEqualTo(NODE_A);
    }

    @Test
    void placedQueryIsAskedUntilItCompletes()
    {
        // Each reservation asks a placed query for its phase until the query completes
        AtomicInteger asked = new AtomicInteger();
        QueryId queryId = nextQueryId();
        Supplier<Phase> phase = () -> {
            asked.incrementAndGet();
            return phases.getOrDefault(queryId, RUNNING);
        };
        assertThat(placements.reserveLeastLoaded(queryId, ALL_NODES, phase)).contains(NODE_A);
        assertThat(asked).hasValue(0);

        reserve(ALL_NODES);
        assertThat(asked).hasValue(1);

        phases.put(queryId, TEARING_DOWN);
        reserve(ALL_NODES);
        assertThat(asked).hasValue(2);

        phases.put(queryId, COMPLETED);
        reserve(ALL_NODES);
        reserve(ALL_NODES);
        assertThat(asked).hasValue(3);
    }

    @Test
    void noCandidates()
    {
        QueryId queryId = nextQueryId();
        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(), phase(queryId))).isEmpty();
    }

    private InternalNode reserve(List<InternalNode> candidates)
    {
        QueryId queryId = nextQueryId();
        return placements.reserveLeastLoaded(queryId, candidates, phase(queryId)).orElseThrow();
    }

    private QueryId reserveOn(InternalNode node)
    {
        QueryId queryId = nextQueryId();
        assertThat(placements.reserveLeastLoaded(queryId, ImmutableList.of(node), phase(queryId))).contains(node);
        return queryId;
    }

    private Supplier<Phase> phase(QueryId queryId)
    {
        return () -> phases.getOrDefault(queryId, RUNNING);
    }

    private QueryId nextQueryId()
    {
        return new QueryId("query_" + nextQueryId.incrementAndGet());
    }
}
