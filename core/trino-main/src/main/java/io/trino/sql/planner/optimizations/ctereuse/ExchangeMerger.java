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
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalContext;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStates;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import io.trino.sql.planner.optimizations.ctereuse.SingleGroupMerger.SingleGroupMergeDecomposition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.toMultimap;
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
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getIdentityMappings;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences.concatenateCheckpointReferences;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.getNextOperation;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.rebasePredicateAndPruneUnsupportedConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.MultiGroupMerger.identifyMultiGroupBranches;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.SemanticEquivalenceUtils.blocksSemanticallyEquivalent;
import static java.util.LinkedHashMap.newLinkedHashMap;
import static java.util.function.Function.identity;

public class ExchangeMerger
        implements SingleGroupMerger.SingleGroupProcessor, MultiGroupMerger.MultiGroupProcessor
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
            if (nextOperation instanceof Exchange exchange && exchange.arguments().size() == 1 && isFullPassthroughFieldSelector(getOnlyElement(exchange.inputFieldSelectors())) && isDeterministic(exchange)) {
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
            Map<Value, Operation> newOperations)
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
        newOperations.put(mergedExchange.result(), mergedExchange);

        return new UnifiedStatesAndCheckpointMapping(
                new UnifiedStates(
                        mergedExchange,
                        branches.stream()
                                .map(traversalState -> new TraversalState(traversalState.traversalContext(), getNextOperation(traversalState.nextOperation().operation(), usesMap).orElseThrow()))
                                .collect(toImmutableList())),
                checkpoints,
                branchToCheckpoint);
    }

    /**
     * Identify groups of identical multi-group Exchange operations representing unions of the same input branches.
     * Exclude exchanges having more than one source in the same group.
     */
    @Override
    public List<MultiGroupMerger.MultiGroupMergeCandidate> identifyMultiGroupMergeCandidates(UnifiedStates newGroup, Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // identify multi-group deterministic exchanges in the new group
        Map<Integer, CteReuse.TraversalState> multiGroupExchanges = identifyMultiGroupBranches(newGroup).stream()
                .collect(toImmutableMap(identity(), newGroup.residualStates()::get))
                .entrySet().stream()
                .filter(entry -> entry.getValue().nextOperation().operation() instanceof Exchange)
                .filter(entry -> isDeterministic(entry.getValue().nextOperation().operation()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        // for each exchange, find all sources of this exchange in the current group and in hanging groups.
        // the current group is marked as "-1".
        Map<Value, GroupAndBranch[]> sourcesMap = newLinkedHashMap(multiGroupExchanges.size());
        // first, initialize the map and fill all references from the current group
        multiGroupExchanges.forEach((index, branch) -> {
            Exchange exchange = (Exchange) branch.nextOperation().operation();
            if (!sourcesMap.containsKey(exchange.result())) {
                GroupAndBranch[] exchangeSources = new GroupAndBranch[exchange.arguments().size()];
                Arrays.fill(exchangeSources, null);
                sourcesMap.put(exchange.result(), exchangeSources);
            }
            sourcesMap.get(exchange.result())[branch.nextOperation().sourceIndex()] = new GroupAndBranch(-1, index);
        });
        // fill all references from hanging groups
        hangingGroups.forEach((hangingGroupId, hangingGroup) -> {
            for (int i = 0; i < hangingGroup.branches().size(); i++) {
                CteReuse.OperationAndIndex nextOperation = hangingGroup.branches().get(i).nextOperation();
                if (sourcesMap.containsKey(nextOperation.operation().result())) {
                    sourcesMap.get(nextOperation.operation().result())[nextOperation.sourceIndex()] = new GroupAndBranch(hangingGroupId, i);
                }
            }
        });

        Map<Value, GroupAndBranch[]> exchangeToSources = sourcesMap.entrySet().stream()
                // filter out exchanges with unresolved sources
                .filter(entry -> Arrays.stream(entry.getValue()).noneMatch(Objects::isNull))
                // filter out exchanges having more than 1 source in the same group
                .filter(entry -> {
                    long referencedGroupsCount = Arrays.stream(entry.getValue())
                            .map(GroupAndBranch::hangingGroupId)
                            .distinct()
                            .count();
                    return referencedGroupsCount == entry.getValue().length;
                })
                // for each exchange that will be merged with other exchanges, the residual predicates must be equal for all sources,
                // and they must be based on pass-through fields, so that the predicate can be re-applied on top of the merged exchange.
                // We cannot compare predicates from different sources for semantic equivalence. We must check if they are identical
                // when rebased onto the merged exchange operation. We filter out exchanges that do not satisfy this condition.
                .filter(entry -> {
                    GroupAndBranch[] sources = entry.getValue();
                    // these are sources of the same exchange. Get the exchange by looking up the first source
                    Exchange exchange = (Exchange) getBranch(sources[0], newGroup, hangingGroups).nextOperation().operation();
                    List<Block> rebasedPredicatesToApply = new ArrayList<>();
                    for (GroupAndBranch source : sources) {
                        Operation unifiedOperation = getUnifiedOperation(source, newGroup, hangingGroups);
                        TraversalState branch = getBranch(source, newGroup, hangingGroups);
                        Block rebasedInputFieldSelector = rebaseBlock(
                                exchange.inputFieldSelectors().get(branch.nextOperation().sourceIndex()),
                                relationRowType(trinoType(unifiedOperation.result().type())),
                                branch.traversalContext().fieldMapping(),
                                nameAllocator).orElseThrow();
                        FieldMapping predicateMapping = getIdentityMappings(rebasedInputFieldSelector);
                        Optional<Block> rebasedPredicateToApply = rebaseBlock(branch.traversalContext().predicateToApply(), relationRowType(trinoType(exchange.result().type())), predicateMapping, nameAllocator);
                        // the residual predicate cannot be rebased onto the merged exchange operation
                        if (rebasedPredicateToApply.isEmpty()) {
                            return false;
                        }
                        rebasedPredicatesToApply.add(rebasedPredicateToApply.get());
                    }
                    for (int i = 1; i < rebasedPredicatesToApply.size(); i++) {
                        if (!blocksSemanticallyEquivalent(rebasedPredicatesToApply.getFirst(), rebasedPredicatesToApply.get(i))) {
                            return false;
                        }
                    }
                    return true;
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        // identify potential matches: subsets of exchanges with respective sources in the same groups
        Multimap<List<Integer>, Value> exchangesBySources = exchangeToSources.entrySet().stream()
                .collect(toMultimap(
                        entry -> Arrays.stream(entry.getValue())
                                .map(GroupAndBranch::hangingGroupId)
                                .collect(toImmutableList()),
                        Map.Entry::getKey,
                        ArrayListMultimap::create));
        // filter out singleton subsets
        List<List<Value>> exchangesWithSameSources = exchangesBySources.asMap().values().stream()
                .filter(exchanges -> exchanges.size() > 1)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());

        // for each subset, find subgroups of exchanges that can be merged
        List<List<Value>> exchangesToMerge = new ArrayList<>();
        for (List<Value> exchanges : exchangesWithSameSources) {
            int[] exchangeSubgroups = new int[exchanges.size()];
            Arrays.fill(exchangeSubgroups, -1);
            Map<Integer, Exchange> subgroupRepresentatives = new HashMap<>();
            for (int i = 0; i < exchanges.size(); i++) {
                GroupAndBranch[] sources = exchangeToSources.get(exchanges.get(i));
                // these are sources of the same exchange. Get the exchange by looking up the first source
                Exchange exchange = (Exchange) getBranch(sources[0], newGroup, hangingGroups).nextOperation().operation();
                List<Block> rebasedInputFieldSelectors = Arrays.stream(sources)
                        .map(source -> {
                            Operation unifiedOperation = getUnifiedOperation(source, newGroup, hangingGroups);
                            TraversalState branch = getBranch(source, newGroup, hangingGroups);
                            return rebaseBlock(
                                    exchange.inputFieldSelectors().get(branch.nextOperation().sourceIndex()),
                                    relationRowType(trinoType(unifiedOperation.result().type())),
                                    branch.traversalContext().fieldMapping(),
                                    nameAllocator).orElseThrow();
                        })
                        .collect(toImmutableList());
                boolean foundMatchingSubgroup = false;
                for (Map.Entry<Integer, Exchange> subgroupRepresentative : subgroupRepresentatives.entrySet()) {
                    // mergeable exchanges have:
                    // - identical attributes
                    // - semantically equivalent input field selectors for each source
                    // - semantically equivalent partitioningBoundArguments, partitioningHashSelector, and orderingSelector
                    // Note: these blocks do not need rebasing. They are based on exchange output type, and the output type stays the same after merging
                    if (subgroupRepresentative.getValue().attributes().equals(exchange.attributes()) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().inputFieldSelectors(), rebasedInputFieldSelectors) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().partitioningBoundArguments(), exchange.partitioningBoundArguments()) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().partitioningHashSelector(), exchange.partitioningHashSelector()) &&
                            blocksSemanticallyEquivalent(subgroupRepresentative.getValue().orderingSelector(), exchange.orderingSelector())) {
                        exchangeSubgroups[i] = subgroupRepresentative.getKey();
                        foundMatchingSubgroup = true;
                        break;
                    }
                }
                // if there is no matching subgroup, start a new subgroup
                if (!foundMatchingSubgroup) {
                    exchangeSubgroups[i] = i;
                    List<Value> unifiedSources = Arrays.stream(sources)
                            .map(source -> getUnifiedOperation(source, newGroup, hangingGroups))
                            .map(Operation::result)
                            .collect(toImmutableList());
                    Exchange rebasedRepresentative = new Exchange(
                            nameAllocator.newName(),
                            unifiedSources,
                            rebasedInputFieldSelectors,
                            exchange.partitioningBoundArguments(),
                            exchange.partitioningHashSelector(),
                            exchange.orderingSelector(),
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
            // extract subgroups
            Multimap<Integer, Integer> subgroups = ArrayListMultimap.create();
            for (int i = 0; i < exchangeSubgroups.length; i++) {
                if (exchangeSubgroups[i] != -1) {
                    subgroups.put(exchangeSubgroups[i], i);
                }
            }
            subgroups.asMap().values().stream()
                    .filter(indexes -> indexes.size() > 1)
                    .map(indexes -> indexes.stream()
                            .map(exchanges::get)
                            .collect(toImmutableList()))
                    .forEach(exchangesToMerge::add);
        }

        // translate subgroups of exchanges to indexes in groups
        return exchangesToMerge.stream()
                .map(exchanges -> {
                    Multimap<Integer, Integer> allSubsetSources = ArrayListMultimap.create();
                    for (Value exchange : exchanges) {
                        GroupAndBranch[] sources = exchangeToSources.get(exchange);
                        Arrays.stream(sources)
                                .forEach(source -> allSubsetSources.put(source.hangingGroupId(), source.branch()));
                    }
                    List<Integer> newGroupBranches = ImmutableList.copyOf(allSubsetSources.get(-1));
                    Map<Integer, List<Integer>> hangingGroupBranches = allSubsetSources.asMap().entrySet().stream()
                            .filter(entry -> entry.getKey() != -1)
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
                    return new MultiGroupMerger.MultiGroupMergeCandidate(newGroupBranches, hangingGroupBranches);
                })
                .collect(toImmutableList());
    }

    private record GroupAndBranch(int hangingGroupId, int branch)
    {}

    private static TraversalState getBranch(GroupAndBranch groupAndBranch, UnifiedStates newGroup, Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups)
    {
        if (groupAndBranch.hangingGroupId() == -1) {
            return newGroup.residualStates().get(groupAndBranch.branch());
        }
        return hangingGroups.get(groupAndBranch.hangingGroupId()).branches().get(groupAndBranch.branch());
    }

    private static Operation getUnifiedOperation(GroupAndBranch groupAndBranch, UnifiedStates newGroup, Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups)
    {
        if (groupAndBranch.hangingGroupId() == -1) {
            return newGroup.unifiedOperation();
        }
        return hangingGroups.get(groupAndBranch.hangingGroupId()).unifiedOperation();
    }

    @Override
    public UnifiedStatesAndCheckpointMapping mergeNextMultiGroupOperation(
            Operation unifiedOperation,
            List<TraversalState> branches,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            List<Integer> hangingGroupsToMerge,
            Map<Integer, MultiGroupMerger.HangingGroup> hangingGroups,
            Multimap<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations)
    {
        // get an arbitrary exchange operation to merge: the one referenced by the first branch
        Exchange arbitraryExchange = (Exchange) branches.getFirst().nextOperation().operation();

        // find all its sources in all groups, and:
        // - collect the arguments for the merged operation: they are the unified operations of source groups
        // - rebase and collect all input field selectors
        Value[] arguments = new Value[arbitraryExchange.arguments().size()];
        Block[] rebasedInputFieldSelectors = new Block[arbitraryExchange.arguments().size()];
        // first, from the current group
        for (TraversalState branch : branches) {
            Operation nextOperation = branch.nextOperation().operation();
            int sourceIndex = branch.nextOperation().sourceIndex();
            if (nextOperation.result().equals(arbitraryExchange.result())) {
                arguments[sourceIndex] = unifiedOperation.result();
                rebasedInputFieldSelectors[sourceIndex] = rebaseBlock(
                        arbitraryExchange.inputFieldSelectors().get(sourceIndex),
                        relationRowType(trinoType(unifiedOperation.result().type())),
                        branch.traversalContext().fieldMapping(),
                        nameAllocator).orElseThrow();
            }
        }
        // then, from the hanging groups
        for (int hangingGroupId : hangingGroupsToMerge) {
            MultiGroupMerger.HangingGroup hangingGroup = hangingGroups.get(hangingGroupId);
            for (TraversalState branch : hangingGroup.branches()) {
                Operation nextOperation = branch.nextOperation().operation();
                int sourceIndex = branch.nextOperation().sourceIndex();
                if (nextOperation.result().equals(arbitraryExchange.result())) {
                    arguments[sourceIndex] = hangingGroup.unifiedOperation().result();
                    rebasedInputFieldSelectors[sourceIndex] = rebaseBlock(
                            arbitraryExchange.inputFieldSelectors().get(sourceIndex),
                            relationRowType(trinoType(hangingGroup.unifiedOperation().result().type())),
                            branch.traversalContext().fieldMapping(),
                            nameAllocator).orElseThrow();
                }
            }
        }

        // build the merged exchange operation
        Exchange mergedExchange = new Exchange(
                nameAllocator.newName(),
                ImmutableList.copyOf(arguments),
                ImmutableList.copyOf(rebasedInputFieldSelectors),
                arbitraryExchange.partitioningBoundArguments(),
                arbitraryExchange.partitioningHashSelector(),
                arbitraryExchange.orderingSelector(),
                EXCHANGE_TYPE.getAttribute(arbitraryExchange.attributes()),
                EXCHANGE_SCOPE.getAttribute(arbitraryExchange.attributes()),
                PARTITIONING_HANDLE.getAttribute(arbitraryExchange.attributes()),
                NULLABLE_VALUES.getAttribute(arbitraryExchange.attributes()),
                REPLICATE_NULLS_AND_ANY.getAttribute(arbitraryExchange.attributes()),
                Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(arbitraryExchange.attributes())),
                Optional.ofNullable(PARTITION_COUNT.getAttribute(arbitraryExchange.attributes())),
                Optional.ofNullable(SORT_ORDERS.getAttribute(arbitraryExchange.attributes())),
                ImmutableList.of());
        newOperations.put(mergedExchange.result(), mergedExchange);

        // for each component exchange, find all sources of this exchange in the current group and in hanging groups.
        // the current group is marked as "-1".
        Map<Value, GroupAndBranch[]> sourcesMap = newLinkedHashMap(branches.size());
        // first, initialize the map and fill all references from the current group
        for (int index = 0; index < branches.size(); index++) {
            TraversalState branch = branches.get(index);
            Exchange componentExchange = (Exchange) branch.nextOperation().operation();
            if (!sourcesMap.containsKey(componentExchange.result())) {
                GroupAndBranch[] exchangeSources = new GroupAndBranch[componentExchange.arguments().size()];
                Arrays.fill(exchangeSources, null);
                sourcesMap.put(componentExchange.result(), exchangeSources);
            }
            sourcesMap.get(componentExchange.result())[branch.nextOperation().sourceIndex()] = new GroupAndBranch(-1, index);
        }
        // fill all references from hanging groups
        hangingGroupsToMerge.stream()
                .forEach(hangingGroupId -> {
                    MultiGroupMerger.HangingGroup hangingGroup = hangingGroups.get(hangingGroupId);
                    for (int i = 0; i < hangingGroup.branches().size(); i++) {
                        CteReuse.OperationAndIndex nextOperation = hangingGroup.branches().get(i).nextOperation();
                        sourcesMap.get(nextOperation.operation().result())[nextOperation.sourceIndex()] = new GroupAndBranch(hangingGroupId, i);
                    }
                });

        // build checkpoints for the unified operation: concatenate checkpoints from all source groups in order of exchange sources.
        // use arbitrary component exchange -- all component exchanges have respective sources in the same groups
        List<Checkpoint> unifiedCheckpoints = Arrays.stream(sourcesMap.values().stream()
                        .findFirst().orElseThrow())
                .map(GroupAndBranch::hangingGroupId)
                .map(hangingGroupId -> {
                    if (hangingGroupId == -1) {
                        return checkpoints;
                    }
                    return hangingGroups.get(hangingGroupId).checkpoints();
                })
                .flatMap(List::stream)
                .collect(toImmutableList());

        // build TraversalStates and branch-to-checkpoint mapping for the component exchange operations
        UnifiedStates currentGroup = new UnifiedStates(unifiedOperation, branches);
        ImmutableList.Builder<TraversalState> traversalStates = ImmutableList.builder();
        ImmutableList.Builder<CheckpointReferences> unifiedBranchToCheckpoint = ImmutableList.builder();
        sourcesMap.forEach((componentExchange, sources) -> {
            // mapping to rebase the next downstream operation from the component exchange onto the merged exchange operation:
            // it is an identity mapping because the merged exchange has the same output type as the component exchanges
            FieldMapping unifiedMapping = FieldMapping.identity(relationRowType(trinoType(mergedExchange.result().type())));

            // fields to prune from the merged exchange operation:
            // it is an empty set because the merged exchange has the same output type as the component exchanges
            Set<Integer> unifiedFieldsToPrune = ImmutableSet.of();

            // the predicate to apply is the same for all exchange sources per the identifyMultiGroupMergeCandidates() method:
            // get the predicate from the first source and rebase it onto the merged exchange operation
            Block unifiedPredicateToApply = rebaseBlock(
                    getBranch(sources[0], currentGroup, hangingGroups).traversalContext().predicateToApply(),
                    relationRowType(trinoType(mergedExchange.result().type())),
                    getIdentityMappings(mergedExchange.inputFieldSelectors().getFirst()),
                    nameAllocator).orElseThrow();

            // the enforced predicate is an intersection of all component enforced predicates, rebased onto the merged exchange operation and pruned to supported fields
            List<Block> rebasedEnforcedPredicates = new ArrayList<>();
            for (int i = 0; i < sources.length; i++) {
                TraversalState branch = getBranch(sources[i], currentGroup, hangingGroups);
                Block rebasedEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(
                        branch.traversalContext().enforcedPredicate(),
                        mergedExchange,
                        getIdentityMappings(mergedExchange.inputFieldSelectors().get(i)),
                        nameAllocator);
                rebasedEnforcedPredicates.add(rebasedEnforcedPredicate);
            }
            Block unifiedEnforcedPredicate = PredicateUtils.intersectPredicates(rebasedEnforcedPredicates, nameAllocator);

            // compute enforced limit for the branch: sum enforced limits from all sources
            OptionalLong unifiedEnforcedLimit = OptionalLong.empty();
            List<OptionalLong> sourceEnforcedLimits = Arrays.stream(sources)
                    .map(source -> getBranch(source, currentGroup, hangingGroups))
                    .map(TraversalState::traversalContext)
                    .map(TraversalContext::enforcedLimit)
                    .collect(toImmutableList());
            if (sourceEnforcedLimits.stream().allMatch(OptionalLong::isPresent)) {
                unifiedEnforcedLimit = OptionalLong.of(sourceEnforcedLimits.stream()
                        .mapToLong(OptionalLong::getAsLong)
                        .sum());
            }

            TraversalContext traversalContext = new TraversalContext(
                    unifiedMapping,
                    unifiedFieldsToPrune,
                    unifiedPredicateToApply,
                    unifiedEnforcedPredicate,
                    unifiedEnforcedLimit);

            TraversalState traversalState = new TraversalState(
                    traversalContext,
                    getNextOperation(getBranch(sources[0], currentGroup, hangingGroups).nextOperation().operation(), usesMap).orElseThrow());
            traversalStates.add(traversalState);

            // combine branch-to-checkpoint mapping for the component exchange: concatenate mappings from all sources in order of sources
            CheckpointReferences componentCheckpointReferences = concatenateCheckpointReferences(Arrays.stream(sources)
                    .map(groupAndBranch -> {
                        int hangingGroupId = groupAndBranch.hangingGroupId();
                        BranchesToCheckpointsMapping mapping;
                        if (hangingGroupId == -1) {
                            mapping = branchToCheckpoint;
                        }
                        else {
                            mapping = hangingGroups.get(hangingGroupId).branchToCheckpoint();
                        }
                        return mapping.getMappingForBranch(groupAndBranch.branch());
                    })
                    .collect(toImmutableList()));
            unifiedBranchToCheckpoint.add(componentCheckpointReferences);
        });

        return new CteReuse.UnifiedStatesAndCheckpointMapping(
                new CteReuse.UnifiedStates(mergedExchange, traversalStates.build()),
                unifiedCheckpoints,
                BranchesToCheckpointsMapping.fromBranchMappings(unifiedBranchToCheckpoint.build()));
    }
}
