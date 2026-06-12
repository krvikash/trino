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
package io.trino.sql.planner.exploratory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Program;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.rule.Context;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.memo.MemoDialect.MEMO;
import static io.trino.sql.dialect.memo.MemoDialect.REUSE;
import static io.trino.sql.planner.exploratory.MemoGroup.singletonGroup;
import static java.util.Objects.requireNonNull;

/**
 * Memo is a data structure that stores a {@link Program} in a compact way. It allows to explore and optimize program efficiently
 * by enabling reuse of computations.
 * <p>
 * Program's operations are grouped into parametrized {@link MemoGroup}s based on logical equivalence. A MemoGroup is thus an abstraction
 * for a function that takes a list of parameters and produces a result. The same result is achieved for each of the group's operations.
 * Operations can refer to other MemoGroups as children. Child MemoGroups can be seen as nested function calls.
 * All parameters referenced by the operations in a group must be listed in the group's parameter list. No outer scope references are allowed.
 * The parameter lineage is tracked via ParameterLineage metadata attached to each edge in the graph.
 * In addition to the grouping based on logical equivalence, equal operations are deduplicated.
 * <ul>
 * <li> rootGroup: the MemoGroup corresponding to the root operation of the program
 * <li> groups: mapping from MemoGroup id to MemoGroup
 * <li> operationToGroup: mapping from MemoOperation to the corresponding MemoGroup id
 * <li> groupToParents: mapping from MemoGroup id to ids of parent groups that reference it as a child
 * <li> operationToRecentVersion: mapping from previous operation versions to recent operation versions.
 * Enables identifying updated operations during and after group merges, in case when operation children references were updated to reflect merged groups
 * <li> groupToRecentId: mapping from previous group ids to recent group ids.
 * Enables identifying updated group ids during and after group merges
 * <li> dialectRegistry: registry of dialects needed to convert between MemoOperations and Operations
 * <li> groupIdAllocator: allocator of unique MemoGroup ids
 * </ul>
 */
public class Memo
{
    private int rootGroup;
    private final Map<Integer, MemoGroup> groups;
    private final Map<MemoOperation, Integer> operationToGroup;
    private final Multimap<Integer, Integer> groupToParents;
    private final Substitution<MemoOperation> operationToRecentVersion = new Substitution<>();
    private final Substitution<Integer> groupToRecentId = new Substitution<>();

    private final DialectRegistry dialectRegistry;
    private final IdAllocator groupIdAllocator;

    Memo(int rootGroup,
            Map<Integer, MemoGroup> groups,
            Map<MemoOperation, Integer> operationToGroup,
            Multimap<Integer, Integer> groupToParents,
            DialectRegistry dialectRegistry,
            IdAllocator groupIdAllocator)
    {
        this.rootGroup = rootGroup;
        this.groups = requireNonNull(groups, "groups is null");
        this.operationToGroup = requireNonNull(operationToGroup, "operationToGroup is null");
        this.groupToParents = requireNonNull(groupToParents, "groupToParents is null");
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
        this.groupIdAllocator = requireNonNull(groupIdAllocator, "groupIdAllocator is null");
    }

    public static Memo forProgram(Program program, DialectRegistry dialectRegistry)
    {
        MemoBuilder builder = new MemoBuilder(dialectRegistry);
        builder.insertProgramRecursively(program);
        return builder.build();
    }

    public int rootGroup()
    {
        return rootGroup;
    }

    public int size()
    {
        return groups.size();
    }

    public MemoGroup getGroup(int groupId)
    {
        return groups.get(groupId);
    }

    public Optional<Integer> getGroupId(MemoOperation operation)
    {
        return Optional.ofNullable(operationToGroup.get(operation));
    }

    public MemoOperation getRecentVersion(MemoOperation operation)
    {
        return operationToRecentVersion.getOrIdentity(operation);
    }

    public int getRecentId(int groupId)
    {
        return groupToRecentId.getOrIdentity(groupId);
    }

    @VisibleForTesting
    Map<Integer, MemoGroup> groups()
    {
        return ImmutableMap.copyOf(groups);
    }

    public DialectRegistry dialectRegistry()
    {
        return dialectRegistry;
    }

    public Context getRuleContext()
    {
        return new Context()
        {
            @Override
            public MemoGroup getGroup(int groupId)
            {
                return Memo.this.getGroup(groupId);
            }

            @Override
            public DialectRegistry getDialectRegistry()
            {
                return dialectRegistry();
            }
        };
    }

    /**
     * Merge the second group into the first group.
     * All references to component groups are updated to reference the merged group.
     * If some operations in Memo become equivalent after this update, their groups are merged recursively.
     * Returns the resulting merged group id, being the id of the first group.
     * The method is cycle-proof.
     */
    int mergeGroups(int firstGroupId, int secondGroupId, MemoChangesCollector changesCollector)
    {
        // a queue of all operations that need to be updated after merge because they reference a group that was merged into another group
        Deque<MemoOperation> operationsToUpdate = new ArrayDeque<>();

        int resultGroupId = mergeGroups(firstGroupId, secondGroupId, operationsToUpdate, changesCollector);

        while (!operationsToUpdate.isEmpty()) {
            MemoOperation operationToUpdate = operationsToUpdate.poll();
            // get the recent version of the operation.
            // due to the non-tree structure of Memo, some operations may be enqueued for update multiple times if they reference multiple merged groups.
            // when the operation is dequeued for update, we must make sure that we use the most recent version of the operation
            // (the most recent version is present in Memo) even though the enqueued operation may be an older version.
            operationToUpdate = getRecentVersion(operationToUpdate);
            // create updated operation with updated references to child groups that were merged
            MemoOperation updatedOperation = updateChildren(operationToUpdate);
            if (updatedOperation.equals(operationToUpdate)) {
                // operation already updated, skip
                continue;
            }
            // record the new version of the operation in operationToRecentVersion
            operationToRecentVersion.put(operationToUpdate, updatedOperation);
            // check if the updated operation already exists in Memo
            Integer foundGroupId = operationToGroup.get(updatedOperation);
            if (foundGroupId != null) {
                // operation already exists in Memo. merge groups recursively and remove previous operation version from the merged group
                int updatedGroupId = operationToGroup.get(operationToUpdate);
                mergeGroups(foundGroupId, updatedGroupId, operationsToUpdate, changesCollector);
                changesCollector.recordOperationSuccession(operationToUpdate, updatedOperation, foundGroupId);
                removeOperation(operationToUpdate, changesCollector);
            }
            else {
                // operation not yet in Memo. replace the previous operation version with the updated operation in its group
                int updatedGroupId = operationToGroup.get(operationToUpdate);
                MemoGroup updatedGroup = groups.get(updatedGroupId);
                Attributes attributesBefore = updatedGroup.attributes();
                updatedGroup.addOperation(updatedOperation, dialectRegistry);
                Attributes attributesAfter = updatedGroup.attributes();
                operationToGroup.put(updatedOperation, updatedGroupId);
                changesCollector.recordOperationInsert(updatedOperation, updatedGroupId);
                if (!attributesBefore.equals(attributesAfter)) {
                    changesCollector.recordGroupAttributesChange(updatedGroupId);
                }
                changesCollector.recordOperationSuccession(operationToUpdate, updatedOperation, updatedGroupId);
                removeOperation(operationToUpdate, changesCollector);
            }
        }

        return resultGroupId;
    }

    private int mergeGroups(int firstGroupId, int secondGroupId, Deque<MemoOperation> operationsToUpdate, MemoChangesCollector changesCollector)
    {
        if (firstGroupId == secondGroupId) {
            return firstGroupId;
        }

        changesCollector.recordGroupMerge(firstGroupId, secondGroupId);

        MemoGroup firstGroup = groups.get(firstGroupId);
        Attributes firstGroupAttributesBefore = firstGroup.attributes();
        MemoGroup secondGroup = groups.get(secondGroupId);
        Attributes secondGroupAttributesBefore = secondGroup.attributes();

        // merge second group into first group so that first group becomes the merge result and first group id becomes the merged group id
        firstGroup.mergeWith(secondGroup, dialectRegistry);
        Attributes mergedGroupAttributes = firstGroup.attributes();
        if (!mergedGroupAttributes.equals(firstGroupAttributesBefore) || !mergedGroupAttributes.equals(secondGroupAttributesBefore)) {
            changesCollector.recordGroupAttributesChange(firstGroupId);
        }

        // update root group if needed
        if (rootGroup == secondGroupId) {
            rootGroup = firstGroupId;
        }

        groups.remove(secondGroupId);
        groupToRecentId.put(secondGroupId, firstGroupId);

        firstGroup.operations().forEach(operation -> operationToGroup.put(operation, firstGroupId));

        // update groupToParents mapping for the merged groups and their child groups
        groupToParents.get(firstGroupId).addAll(groupToParents.get(secondGroupId));
        // in a cyclic memo, either of the merged groups might have secondGroupId as parent
        // remap those stale references into firstGroupId
        if (groupToParents.get(firstGroupId).contains(secondGroupId)) {
            groupToParents.put(firstGroupId, firstGroupId);
            groupToParents.remove(firstGroupId, secondGroupId);
        }
        groupToParents.removeAll(secondGroupId);
        firstGroup.getChildGroups()
                .forEach(childGroupId -> {
                    // in a cyclic memo, child reference might point back to the same group
                    // references to secondGroupId are stale and should be translated to firstGroupId
                    if (childGroupId.equals(secondGroupId)) {
                        childGroupId = firstGroupId;
                    }
                    groupToParents.put(childGroupId, firstGroupId);
                    groupToParents.remove(childGroupId, secondGroupId);
                });

        // identify and enqueue operations that need to be updated
        // in a cyclic memo, parents of firstGroupId might include the same group
        // in such a case, we will correctly enqueue the current group's operations for update
        groupToParents.get(firstGroupId).stream()
                .map(groups::get)
                .flatMap(group -> group.operations().stream())
                .filter(operation -> operation.children().stream()
                        .anyMatch(child -> child instanceof GroupChild groupChild && groupChild.groupId() == secondGroupId))
                .forEach(operationsToUpdate::add);

        return firstGroupId;
    }

    MemoOperation updateChildren(MemoOperation operation)
    {
        return operation.remapChildren(groupToRecentId::getOrIdentity);
    }

    int insertOperation(MemoOperation operation, MemoChangesCollector changesCollector)
    {
        Integer groupId = operationToGroup.get(operation);
        if (groupId == null) {
            // this operation does not exist yet in memo, create a new group
            groupId = groupIdAllocator.newId();
            MemoGroup group = singletonGroup(operation, dialectRegistry);
            groups.put(groupId, group);
            operationToGroup.put(operation, groupId);
            changesCollector.recordSingletonGroupCreate(operation, groupId);
            for (int childGroupId : group.getChildGroups()) {
                groupToParents.put(childGroupId, groupId);
            }
        }
        else {
            // found this operation in memo
            MemoGroup group = groups.get(groupId);
            MemoOperation previousOperation = group.operations().stream()
                    .filter(existingOperation -> existingOperation.equals(operation))
                    .findFirst()
                    .orElseThrow();
            // update group attributes with the new operation's attributes
            Attributes attributesBefore = group.attributes();
            group.addOperation(operation, dialectRegistry);
            if (!previousOperation.attributes().equals(operation.attributes())) {
                changesCollector.recordOperationAttributesChange(previousOperation, operation, groupId);
            }
            if (!group.attributes().equals(attributesBefore)) {
                changesCollector.recordGroupAttributesChange(groupId);
            }
        }

        return groupId;
    }

    /**
     * Remove the given operation from the Memo.
     * Note: this method will remove the operation based on operation equals(), so without considering derived attributes.
     */
    void removeOperation(MemoOperation operation, MemoChangesCollector changesCollector)
    {
        checkArgument(operationToGroup.containsKey(operation), "Removed operation not found in Memo: %s", operation);
        int groupId = operationToGroup.get(operation);
        MemoGroup group = groups.get(groupId);
        Set<Integer> childGroups = group.getChildGroups();
        group.removeOperation(operation, dialectRegistry);
        operationToGroup.remove(operation);
        changesCollector.recordOperationRemove(operation, groupId);
        Set<Integer> remainingChildGroups = group.getChildGroups();
        // update groupToParents mapping for child groups that no longer reference the group after the operation removal
        Sets.difference(childGroups, remainingChildGroups)
                .forEach(childGroupId -> groupToParents.remove(childGroupId, groupId));
    }

    void pruneOrphanedGroups(MemoChangesCollector changesCollector)
    {
        Set<Integer> referencedGroups = ImmutableSet.copyOf(reachablePostOrder());
        groups.forEach((groupId, group) -> {
            if (!referencedGroups.contains(groupId)) {
                changesCollector.recordGroupPrune(groupId, group.operations());
            }
        });
        groups.keySet().retainAll(referencedGroups);
        operationToGroup.entrySet().removeIf(entry -> !referencedGroups.contains(entry.getValue()));
        groupToParents.entries().removeIf(entry -> !referencedGroups.contains(entry.getKey()) || !referencedGroups.contains(entry.getValue()));
    }

    @VisibleForTesting
    public void validateGroupToParentsMapping()
    {
        Multimap<Integer, Integer> actualGroupToParents = LinkedHashMultimap.create();
        groups.forEach((groupId, group) -> {
            Set<Integer> childGroups = group.getChildGroups();
            childGroups.forEach(childGroupId -> actualGroupToParents.put(childGroupId, groupId));
        });
        checkArgument(actualGroupToParents.equals(groupToParents), "Invalid groups to parent mapping");
    }

    /**
     * List all reachable groups in memo in reverse post-order without duplicates,
     * resulting either from diamond shape or from cycles.
     * Child groups are listed in order of reference.
     */
    public List<Integer> reachableReversePostOrder()
    {
        return ((ImmutableList<Integer>) reachablePostOrder()).reverse();
    }

    public List<Integer> reachablePostOrder()
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        getReachableGroupsDeduplicated(builder, new HashSet<>(), new HashSet<>(), rootGroup());
        return builder.build();
    }

    private void getReachableGroupsDeduplicated(ImmutableList.Builder<Integer> builder, Set<Integer> visitedGroups, Set<Integer> activeGroups, int groupId)
    {
        if (visitedGroups.contains(groupId) || activeGroups.contains(groupId)) {
            return;
        }

        activeGroups.add(groupId);
        List<Integer> childGroupIds = groups.get(groupId).operations().stream()
                .map(MemoOperation::children)
                .flatMap(List::stream)
                .filter(GroupChild.class::isInstance)
                .map(GroupChild.class::cast)
                .map(GroupChild::groupId)
                .collect(toImmutableList());

        for (int childGroupId : childGroupIds.reversed()) {
            getReachableGroupsDeduplicated(builder, visitedGroups, activeGroups, childGroupId);
        }

        activeGroups.remove(groupId);
        builder.add(groupId);
        visitedGroups.add(groupId);
    }

    /**
     * List all groups in memo: reachable groups first (in reverse post-order, root first),
     * then all orphaned (unreachable) groups. Orphaned groups are traversed as a DFS forest -
     * each subgraph is entered from a group with no parent within the orphaned set (a subgraph
     * "root") when one exists, falling back to the remaining groups (e.g. purely cyclic
     * subgraphs, which have no root) in group-id order. Every group appears exactly once.
     */
    public List<Integer> fullReversePostOrder()
    {
        List<Integer> reachable = reachableReversePostOrder();
        return ImmutableList.<Integer>builder()
                .addAll(reachable)
                .addAll(orphanedReversePostOrder(ImmutableSet.copyOf(reachable)))
                .build();
    }

    private List<Integer> orphanedReversePostOrder(Set<Integer> reachable)
    {
        Set<Integer> orphaned = new HashSet<>(groups.keySet());
        orphaned.removeAll(reachable);
        if (orphaned.isEmpty()) {
            return ImmutableList.of();
        }

        // Prefer subgraph roots (no parent within the orphaned set) first, then the rest
        // (e.g. purely cyclic subgraphs). Sort by id within each part for deterministic output.
        List<Integer> startOrder = Stream.concat(
                        orphaned.stream().filter(groupId -> !hasParentIn(groupId, orphaned)).sorted(),
                        orphaned.stream().filter(groupId -> hasParentIn(groupId, orphaned)).sorted())
                .collect(toImmutableList());

        // Shared visited seeded with the reachable groups, so cross-edges from an orphan into
        // the reachable part do not re-list reachable groups. Reverse each subgraph's post-order
        // so its entry group prints first, and keep subgraphs in startOrder.
        ImmutableList.Builder<Integer> result = ImmutableList.builder();
        Set<Integer> visited = new HashSet<>(reachable);
        Set<Integer> active = new HashSet<>();
        for (int startGroup : startOrder) {
            if (visited.contains(startGroup)) {
                continue;
            }
            ImmutableList.Builder<Integer> subgraph = ImmutableList.builder();
            getReachableGroupsDeduplicated(subgraph, visited, active, startGroup);
            result.addAll(((ImmutableList<Integer>) subgraph.build()).reverse());
        }
        return result.build();
    }

    private boolean hasParentIn(int groupId, Set<Integer> candidates)
    {
        return groupToParents.get(groupId).stream().anyMatch(candidates::contains);
    }

    boolean isReuse(int groupId)
    {
        MemoGroup group = groups.get(groupId);
        if (group.operations().size() > 1) {
            return false;
        }
        MemoOperation operation = getOnlyElement(group.operations());
        return operation.dialect().equals(MEMO) && operation.operationId().name().equals(REUSE);
    }

    int reusedGroup(int groupId)
    {
        checkArgument(isReuse(groupId), "Group %s is not a reuse group", groupId);
        MemoOperation operation = getOnlyElement(groups.get(groupId).operations());
        return ((GroupChild) getOnlyElement(operation.children())).groupId();
    }

    public static class IdAllocator
    {
        private int nextId;

        public int newId()
        {
            return nextId++;
        }
    }
}
