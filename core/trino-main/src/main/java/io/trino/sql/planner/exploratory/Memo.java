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
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Program;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        MemoBuilder builder = new MemoBuilder(dialectRegistry, new IdAllocator());
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

    /**
     * Merge the second group into the first group.
     * All references to component groups are updated to reference the merged group.
     * If some operations in Memo become equivalent after this update, their groups are merged recursively.
     * Returns the resulting merged group id, being the id of the first group.
     */
    int mergeGroups(int firstGroupId, int secondGroupId)
    {
        // a queue of all operations that need to be updated after merge because they reference a group that was merged into another group
        Deque<MemoOperation> operationsToUpdate = new ArrayDeque<>();

        int resultGroupId = mergeGroups(firstGroupId, secondGroupId, operationsToUpdate);

        while (!operationsToUpdate.isEmpty()) {
            MemoOperation operationToUpdate = operationsToUpdate.poll();
            // get the recent version of the operation.
            // due to the DAG structure of Memo, some operations may be enqueued for update multiple times if they reference multiple merged groups.
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
                mergeGroups(foundGroupId, updatedGroupId, operationsToUpdate);
                removeOperation(operationToUpdate);
            }
            else {
                // operation not yet in Memo. replace the previous operation version with the updated operation in its group
                int updatedGroupId = operationToGroup.get(operationToUpdate);
                MemoGroup updatedGroup = groups.get(updatedGroupId);
                updatedGroup.addOperation(updatedOperation, dialectRegistry);
                operationToGroup.put(updatedOperation, updatedGroupId);
                removeOperation(operationToUpdate);
            }
        }

        return resultGroupId;
    }

    private int mergeGroups(int firstGroupId, int secondGroupId, Deque<MemoOperation> operationsToUpdate)
    {
        if (firstGroupId == secondGroupId) {
            return firstGroupId;
        }
        // TODO Remove this temporary guard when Memo merge and traversal become cycle-safe.
        // It is expensive: it walks the reachable Memo graph and runs for every merge, including recursive merges.
        checkArgument(
                !achievableFrom(firstGroupId, secondGroupId, new HashSet<>()) && !achievableFrom(secondGroupId, firstGroupId, new HashSet<>()),
                "Merging groups would create a cycle");

        MemoGroup firstGroup = groups.get(firstGroupId);
        MemoGroup secondGroup = groups.get(secondGroupId);
        // merge second group into first group so that first group becomes the merge result and first group id becomes the merged group id
        firstGroup.mergeWith(secondGroup, dialectRegistry);

        // update root group if needed
        if (rootGroup == secondGroupId) {
            rootGroup = firstGroupId;
        }

        groups.remove(secondGroupId);
        groupToRecentId.put(secondGroupId, firstGroupId);

        firstGroup.operations().forEach(operation -> operationToGroup.put(operation, firstGroupId));

        // update groupToParents mapping for the merged groups and their child groups
        groupToParents.get(firstGroupId).addAll(groupToParents.get(secondGroupId));
        groupToParents.removeAll(secondGroupId);
        firstGroup.getChildGroups()
                .forEach(childGroupId -> {
                    groupToParents.put(childGroupId, firstGroupId);
                    groupToParents.remove(childGroupId, secondGroupId);
                });

        // identify and enqueue operations that need to be updated
        groupToParents.get(firstGroupId).stream()
                .map(groups::get)
                .flatMap(group -> group.operations().stream())
                .filter(operation -> operation.children().stream()
                        .anyMatch(child -> child instanceof GroupChild groupChild && groupChild.groupId() == secondGroupId))
                .forEach(operationsToUpdate::add);

        return firstGroupId;
    }

    private boolean achievableFrom(int sourceGroupId, int searchedGroupId, Set<Integer> visitedGroups)
    {
        int recentSourceGroupId = getRecentId(sourceGroupId);
        int recentSearchedGroupId = getRecentId(searchedGroupId);
        if (recentSourceGroupId == recentSearchedGroupId) {
            return true;
        }
        if (!visitedGroups.add(recentSourceGroupId)) {
            return false;
        }

        MemoGroup sourceGroup = groups.get(recentSourceGroupId);
        if (sourceGroup == null) {
            return false;
        }

        return sourceGroup.getChildGroups().stream()
                .anyMatch(childGroupId -> achievableFrom(childGroupId, recentSearchedGroupId, visitedGroups));
    }

    MemoOperation updateChildren(MemoOperation operation)
    {
        return operation.remapChildren(groupToRecentId::getOrIdentity);
    }

    int insertOperation(MemoOperation operation)
    {
        Integer groupId = operationToGroup.get(operation);
        if (groupId == null) {
            // this operation does not exist yet in memo, create a new group
            groupId = groupIdAllocator.newId();
            MemoGroup group = singletonGroup(operation, dialectRegistry);
            groups.put(groupId, group);
            operationToGroup.put(operation, groupId);
            for (int childGroupId : group.getChildGroups()) {
                groupToParents.put(childGroupId, groupId);
            }
        }
        else {
            // found this operation in memo
            MemoGroup group = groups.get(groupId);
            // update group attributes with the new operation's attributes
            group.addOperation(operation, dialectRegistry);
        }

        return groupId;
    }

    /**
     * Remove the given operation from the Memo.
     * Note: this method will remove the operation based on operation equals(), so without considering derived attributes.
     */
    void removeOperation(MemoOperation operation)
    {
        checkArgument(operationToGroup.containsKey(operation), "Removed operation not found in Memo: %s", operation);
        int groupId = operationToGroup.get(operation);
        MemoGroup group = groups.get(groupId);
        Set<Integer> childGroups = group.getChildGroups();
        group.removeOperation(operation, dialectRegistry);
        operationToGroup.remove(operation);
        Set<Integer> remainingChildGroups = group.getChildGroups();
        // update groupToParents mapping for child groups that no longer reference the group after the operation removal
        Sets.difference(childGroups, remainingChildGroups)
                .forEach(childGroupId -> groupToParents.remove(childGroupId, groupId));
    }

    void pruneOrphanedGroups()
    {
        Set<Integer> referencedGroups = ImmutableSet.copyOf(reverseTopologicalOrder());
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
     * List all groups in memo in topological order without duplicates.
     * A child group should appear after all references to it.
     * Child groups are listed in order of reference.
     */
    public List<Integer> topologicalOrder()
    {
        return ((ImmutableList<Integer>) reverseTopologicalOrder()).reverse();
    }

    public List<Integer> reverseTopologicalOrder()
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        getAllGroupsDeduplicated(builder, new HashSet<>(), rootGroup());
        return builder.build();
    }

    private void getAllGroupsDeduplicated(ImmutableList.Builder<Integer> builder, Set<Integer> visitedGroups, int groupId)
    {
        if (visitedGroups.contains(groupId)) {
            return;
        }

        List<Integer> childGroupIds = groups.get(groupId).operations().stream()
                .map(MemoOperation::children)
                .flatMap(List::stream)
                .filter(GroupChild.class::isInstance)
                .map(GroupChild.class::cast)
                .map(GroupChild::groupId)
                .collect(toImmutableList());

        for (int childGroupId : childGroupIds.reversed()) {
            getAllGroupsDeduplicated(builder, visitedGroups, childGroupId);
        }

        builder.add(groupId);
        visitedGroups.add(groupId);
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
