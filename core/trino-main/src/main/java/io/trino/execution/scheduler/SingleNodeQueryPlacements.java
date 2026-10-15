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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.execution.QueryStateMachine;
import io.trino.node.InternalNode;
import io.trino.spi.QueryId;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

/**
 * Places each single-node query on the worker running the fewest of them.
 */
@ThreadSafe
public class SingleNodeQueryPlacements
{
    /**
     * A placed query is tearing down from when it is done until its final query info.
     */
    public enum Phase
    {
        RUNNING,
        TEARING_DOWN,
        COMPLETED;

        public static Phase of(QueryStateMachine queryStateMachine)
        {
            if (queryStateMachine.getFinalQueryInfo().isPresent()) {
                return COMPLETED;
            }
            if (queryStateMachine.isDone()) {
                return TEARING_DOWN;
            }
            return RUNNING;
        }
    }

    @GuardedBy("this")
    private final Map<QueryId, Placement> placements = new HashMap<>();

    /**
     * Reserves the candidate with the fewest running queries, then the fewest tearing down.
     */
    public synchronized Optional<InternalNode> reserveLeastLoaded(QueryId queryId, List<InternalNode> candidates, Supplier<Phase> phase)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(candidates, "candidates is null");
        requireNonNull(phase, "phase is null");

        Placement placed = placements.get(queryId);
        if (placed != null && candidates.contains(placed.node())) {
            return Optional.of(placed.node());
        }
        // A placed query keeps tearing down on its node when there is nothing to move it to
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        Set<InternalNode> formerNodes = placed == null ? ImmutableSet.of() : ImmutableSet.<InternalNode>builder().addAll(placed.formerNodes()).add(placed.node()).build();
        release(queryId);

        Multiset<InternalNode> running = HashMultiset.create();
        Multiset<InternalNode> tearingDown = HashMultiset.create();
        // A moved query may still have tasks on its former nodes until the remote task error timeout
        tearingDown.addAll(formerNodes);
        Iterator<Placement> iterator = placements.values().iterator();
        while (iterator.hasNext()) {
            Placement placement = iterator.next();
            switch (placement.phase().get()) {
                case RUNNING -> {
                    running.add(placement.node());
                    tearingDown.addAll(placement.formerNodes());
                }
                case TEARING_DOWN -> {
                    tearingDown.add(placement.node());
                    tearingDown.addAll(placement.formerNodes());
                }
                case COMPLETED -> iterator.remove();
            }
        }
        Comparator<InternalNode> byLoad = comparingInt((InternalNode node) -> running.count(node)).thenComparingInt(tearingDown::count);
        InternalNode best = candidates.getFirst();
        for (InternalNode candidate : candidates) {
            if (byLoad.compare(candidate, best) < 0) {
                best = candidate;
            }
        }
        placements.put(queryId, new Placement(best, formerNodes, phase));
        return Optional.of(best);
    }

    public synchronized void release(QueryId queryId)
    {
        placements.remove(requireNonNull(queryId, "queryId is null"));
    }

    /**
     * @param formerNodes the nodes the query was placed on before, which it may still have tasks on
     */
    private record Placement(InternalNode node, Set<InternalNode> formerNodes, Supplier<Phase> phase)
    {
        private Placement
        {
            requireNonNull(node, "node is null");
            formerNodes = ImmutableSet.copyOf(formerNodes);
            requireNonNull(phase, "phase is null");
        }
    }
}
