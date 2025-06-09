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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.trino.Attributes.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.Attributes.NULLABLE_VALUES;
import static io.trino.sql.dialect.trino.Attributes.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.Attributes.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;

public class ExchangeMerger
        implements SingleGroupMerger.SingleGroupProcessor
{
    @Override
    public boolean processes(Operation operation)
    {
        return operation instanceof Exchange;
    }

    /**
     * Identify groups of identical, single-source, pass-through Exchange operations.
     */
    @Override
    public SingleGroupMergeDecomposition identifySingleGroupMergeCandidates(UnifiedStates unifiedStates, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<TraversalState> branches = unifiedStates.residualStates();
        Operation unifiedOperation = unifiedStates.unifiedOperation();

        int[] exchangeSubgroups = new int[branches.size()];
        Arrays.fill(exchangeSubgroups, -1);
        Map<Integer, Exchange> subgroupRepresentatives = new HashMap<>();

        for (int i = 0; i < branches.size(); i++) {
            TraversalState branch = branches.get(i);
            Operation nextOperation = branch.nextOperation().operation();
            if (nextOperation instanceof Exchange exchange && exchange.arguments().size() == 1 && isFullPassthroughFieldSelector(exchange.regions().getFirst().getOnlyBlock()) && isDeterministic(exchange)) {
                // rebase each block on the unified input type. For the input field selector, get a full-passthrough selector on the unified operation type
                ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
                rebasedBlocksBuilder.add(getFullPassthroughFieldSelector("^inputSelector", relationRowType(trinoType(unifiedOperation.result().type())), nameAllocator));
                exchange.regions().subList(1, exchange.regions().size()).stream()
                        .map(Region::getOnlyBlock)
                        .map(block -> rebaseBlock(block, relationRowType(trinoType(unifiedOperation.result().type())), branch.traversalContext().fieldMapping(), nameAllocator))
                        .map(Optional::orElseThrow)
                        .forEach(rebasedBlocksBuilder::add);
                List<Block> rebasedBlocks = rebasedBlocksBuilder.build();
                // find a matching Exchange subgroup. A matching exchange has equal attributes and semantically equivalent regions
                boolean foundMatchingSubgroup = false;
                for (Map.Entry<Integer, Exchange> subgroupRepresentative : subgroupRepresentatives.entrySet()) {
                    if (subgroupRepresentative.getValue().attributes().equals(exchange.attributes()) &&
                            blocksSemanticallyEquivalent(
                                    subgroupRepresentative.getValue().regions().stream()
                                            .map(Region::getOnlyBlock)
                                            .collect(toImmutableList()),
                                    rebasedBlocks)) {
                        exchangeSubgroups[i] = subgroupRepresentative.getKey();
                        foundMatchingSubgroup = true;
                        break;
                    }
                }
                // if there is no matching subgroup, start a new subgroup
                if (!foundMatchingSubgroup) {
                    exchangeSubgroups[i] = i;
                    Exchange rebasedRepresentative = new Exchange(
                            nameAllocator.newName(),
                            ImmutableList.of(unifiedOperation.result()),
                            ImmutableList.of(rebasedBlocks.getFirst()),
                            rebasedBlocks.get(1),
                            rebasedBlocks.get(2),
                            rebasedBlocks.get(3),
                            EXCHANGE_TYPE.getAttribute(exchange.attributes()),
                            EXCHANGE_SCOPE.getAttribute(exchange.attributes()),
                            PARTITIONING_HANDLE.getAttribute(exchange.attributes()),
                            NULLABLE_VALUES.getAttribute(exchange.attributes()),
                            REPLICATE_NULLS_AND_ANY.getAttribute(exchange.attributes()),
                            Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(exchange.attributes())),
                            Optional.ofNullable(PARTITION_COUNT.getAttribute(exchange.attributes())),
                            Optional.ofNullable(SORT_ORDERS.getAttribute(exchange.attributes())),
                            ImmutableList.of());
                    subgroupRepresentatives.put(i, rebasedRepresentative);
                }
            }
        }
        // extract subgroups
        Multimap<Integer, Integer> subgroups = ArrayListMultimap.create();
        for (int i = 0; i < exchangeSubgroups.length; i++) {
            if (exchangeSubgroups[i] != -1) {
                subgroups.put(exchangeSubgroups[i], i);
            }
        }

        // return subgroups with two or more elements
        return new SingleGroupMergeDecomposition(subgroups.asMap().values().stream()
                .filter(indexes -> indexes.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList()));
    }

    /**
     * Merge a group of Exchange operations. Must be compatible with identifyExchangesToMerge().
     */
    @Override
    public UnifiedStatesAndCheckpointMapping mergeNextSingleGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            Multimap<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            ImmutableSet.Builder<Operation> newOperations)
    {
        checkArgument(branches.size() > 1, "must provide at least two branches for merging");
        // TODO reuse the code or make it stateful so that we can record and use the rebased operation from the identifyExchangesToMerge() method
        Exchange exchange = (Exchange) branches.getFirst().nextOperation().operation();
        // rebase each block on the unified input type. For the input field selector, get a full-passthrough selector on the unified operation type
        ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
        rebasedBlocksBuilder.add(getFullPassthroughFieldSelector("^inputSelector", relationRowType(trinoType(unifiedOperation.result().type())), nameAllocator));
        exchange.regions().subList(1, exchange.regions().size()).stream()
                .map(Region::getOnlyBlock)
                .map(block -> rebaseBlock(block, relationRowType(trinoType(unifiedOperation.result().type())), branches.getFirst().traversalContext().fieldMapping(), nameAllocator))
                .map(Optional::orElseThrow)
                .forEach(rebasedBlocksBuilder::add);
        List<Block> rebasedBlocks = rebasedBlocksBuilder.build();

        Exchange mergedExchange = new Exchange(
                nameAllocator.newName(),
                ImmutableList.of(unifiedOperation.result()),
                ImmutableList.of(rebasedBlocks.getFirst()),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                rebasedBlocks.get(3),
                EXCHANGE_TYPE.getAttribute(exchange.attributes()),
                EXCHANGE_SCOPE.getAttribute(exchange.attributes()),
                PARTITIONING_HANDLE.getAttribute(exchange.attributes()),
                NULLABLE_VALUES.getAttribute(exchange.attributes()),
                REPLICATE_NULLS_AND_ANY.getAttribute(exchange.attributes()),
                Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(exchange.attributes())),
                Optional.ofNullable(PARTITION_COUNT.getAttribute(exchange.attributes())),
                Optional.ofNullable(SORT_ORDERS.getAttribute(exchange.attributes())),
                ImmutableList.of());
        newOperations.add(mergedExchange);

        return new UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(
                        mergedExchange,
                        branches.stream()
                                .map(traversalState -> new TraversalState(traversalState.traversalContext(), getNextOperation(traversalState.nextOperation().operation(), usesMap).orElseThrow()))
                                .collect(toImmutableList())),
                checkpoints,
                branchToCheckpoint);
    }
}
