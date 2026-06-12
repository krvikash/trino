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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.exploratory.Candidate.CandidateKey;
import io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.exploratory.Candidate.CandidateKey.candidateKey;
import static io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey.stateKey;
import static io.trino.sql.planner.exploratory.GroupOptimizationState.mergeStates;
import static io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey.alternativeKey;
import static io.trino.sql.planner.exploratory.OperationImplementationAlternative.alternative;
import static java.util.Objects.requireNonNull;

public class OptimizationStore<C extends Cost, R extends Requirement, P extends Property>
{
    private final Memo memo;
    private final PhysicalModel<C, R, P> physicalModel;

    private final Map<Integer, Set<StateKey<R>>> groupToStateKeys = new LinkedHashMap<>();
    private final Map<StateKey<R>, GroupOptimizationState<C, R, P>> stateKeyToState = new LinkedHashMap<>();
    private final Map<MemoOperation, Set<AlternativeKey<R>>> operationToAlternativeKeys = new LinkedHashMap<>();
    private final Map<AlternativeKey<R>, OperationImplementationAlternative<C, R, P>> alternativeKeyToAlternative = new LinkedHashMap<>();
    private final Map<AlternativeKey<R>, Set<CandidateKey<R>>> alternativeToCandidateKeys = new LinkedHashMap<>();
    private final Map<CandidateKey<R>, Candidate<C, R, P>> candidateKeyToCandidate = new LinkedHashMap<>();
    private final Map<StateKey<R>, Set<AlternativeKey<R>>> stateToParentAlternatives = new LinkedHashMap<>();
    private final Map<AlternativeKey<R>, Set<DirtyReason>> dirtyAlternatives = new LinkedHashMap<>();

    public enum DirtyReason
    {
        CHILD_ATTRIBUTES_CHANGED,
        CHILD_COST_CHANGED,
        GROUP_ATTRIBUTES_CHANGED,
        INHERITED,
    }

    // ** STABLE-STATE API for interacting with the current Memo state **

    public OptimizationStore(Memo memo, PhysicalModel<C, R, P> physicalModel)
    {
        this.memo = requireNonNull(memo, "memo is null");
        this.physicalModel = requireNonNull(physicalModel, "physicalModel is null");
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return whether a group optimization state exists for the given key.
     * <p>
     * The key must use the current group id.
     */
    public boolean containsState(StateKey<R> stateKey)
    {
        verifyRecent(stateKey);
        return stateKeyToState.containsKey(stateKey);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return the group optimization state for the given key.
     * <p>
     * The key must use the current group id and must already exist.
     */
    public GroupOptimizationState<C, R, P> getState(StateKey<R> stateKey)
    {
        verifyRecent(stateKey);
        GroupOptimizationState<C, R, P> state = stateKeyToState.get(stateKey);
        checkArgument(state != null, "state not found: %s", stateKey);
        return state;
    }

    public GroupOptimizationState<C, R, P> getState(int groupId, R requirement)
    {
        return getState(stateKey(groupId, requirement, physicalModel.requirements()));
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Record a new group optimization state.
     * <p>
     * The key must use the current group id and must not already exist.
     * The state must be empty (no winners).
     */
    public void putNewState(GroupOptimizationState<C, R, P> state)
    {
        verifyRecent(state.key());
        checkArgument(!stateKeyToState.containsKey(state.key()), "state already exists");
        checkArgument(state.winners().isEmpty(), "new state must not have winners");

        groupToStateKeys.computeIfAbsent(state.groupId(), _ -> new LinkedHashSet<>()).add(state.key());
        stateKeyToState.put(state.key(), state);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Replace the group optimization state stored for the given key.
     * <p>
     * The key must use the current group id and must already exist.
     * This updates only the group optimization state layer. It does not reconcile or modify
     * underlying alternatives, candidates, parent dependencies, or dirty reasons.
     */
    public void replaceState(GroupOptimizationState<C, R, P> state)
    {
        verifyRecent(state.key());
        checkArgument(stateKeyToState.containsKey(state.key()), "state not found: %s", state.key());
        stateKeyToState.put(state.key(), state);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return whether an implementation alternative exists for the given key.
     * <p>
     * The alternative's operation must be current in Memo.
     */
    public boolean containsAlternative(AlternativeKey<R> alternativeKey)
    {
        verifyRecent(alternativeKey);
        return alternativeKeyToAlternative.containsKey(alternativeKey);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return the implementation alternative for the given key.
     * <p>
     * The alternative's operation must be current in Memo and the alternative
     * must already exist.
     */
    public OperationImplementationAlternative<C, R, P> getAlternative(AlternativeKey<R> alternativeKey)
    {
        verifyRecent(alternativeKey);
        OperationImplementationAlternative<C, R, P> alternative = alternativeKeyToAlternative.get(alternativeKey);
        checkArgument(alternative != null, "alternative not found: %s", alternativeKey);
        return alternative;
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Record a newly created implementation alternative.
     * <p>
     * The alternative's operation must be current in Memo and the alternative
     * key must be new. This method also updates the reverse dependency index
     * from child group optimization states to the parent alternative. It does not create
     * candidates for the alternative.
     */
    public void putNewAlternative(OperationImplementationAlternative<C, R, P> alternative)
    {
        AlternativeKey<R> alternativeKey = alternative.key();
        verifyRecent(alternativeKey);
        checkArgument(!alternativeKeyToAlternative.containsKey(alternativeKey), "alternative already exists: %s", alternativeKey);

        alternativeKeyToAlternative.put(alternativeKey, alternative);
        operationToAlternativeKeys.computeIfAbsent(alternative.operation(), _ -> new LinkedHashSet<>()).add(alternativeKey);
        for (StateKey<R> childStateKey : getChildStates(alternativeKey)) {
            stateToParentAlternatives.computeIfAbsent(childStateKey, _ -> new LinkedHashSet<>()).add(alternativeKey);
        }
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Replace the implementation alternative stored for the given key.
     * The alternative's operation must be current in Memo and the alternative
     * must already exist.
     * <p>
     * This method updates the alternative layer, clears dirty reasons for the
     * alternative, and removes all currently stored candidates backed by that
     * alternative key.
     * <p>
     * Current design note: this invalidates candidates not only for the
     * currently processed request state, but also for other states that use the
     * same alternative. Those states are expected to be already enqueued for
     * reprocessing and will recreate their missing candidates when processed.
     * This is an intentional simplification and may need refinement later if we
     * want more local state refresh.
     */
    public void replaceAlternative(OperationImplementationAlternative<C, R, P> alternative)
    {
        AlternativeKey<R> alternativeKey = alternative.key();
        verifyRecent(alternativeKey);
        checkArgument(alternativeKeyToAlternative.containsKey(alternativeKey), "alternative not found: %s", alternativeKey);

        Set<CandidateKey<R>> candidatesToRemove = ImmutableSet.copyOf(alternativeToCandidateKeys.getOrDefault(alternativeKey, ImmutableSet.of()));
        int groupId = memo.getGroupId(alternative.operation()).orElseThrow();

        alternativeKeyToAlternative.put(alternativeKey, alternative);
        candidateKeyToCandidate.keySet().removeAll(candidatesToRemove);
        alternativeToCandidateKeys.remove(alternativeKey);
        dirtyAlternatives.remove(alternativeKey);

        groupToStateKeys.getOrDefault(groupId, ImmutableSet.of())
                .forEach(stateKey -> stateKeyToState.put(stateKey, stateKeyToState.get(stateKey).removeCandidates(candidatesToRemove)));
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return whether a candidate exists for the given key.
     * <p>
     * The candidate's alternative must be current in Memo.
     */
    public boolean containsCandidate(CandidateKey<R> candidateKey)
    {
        verifyRecent(candidateKey.alternativeKey());
        return candidateKeyToCandidate.containsKey(candidateKey);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return the candidate for the given key.
     * <p>
     * The candidate's alternative must be current in Memo and the candidate
     * must already exist.
     */
    public Candidate<C, R, P> getCandidate(CandidateKey<R> candidateKey)
    {
        verifyRecent(candidateKey.alternativeKey());
        Candidate<C, R, P> candidate = candidateKeyToCandidate.get(candidateKey);
        checkArgument(candidate != null, "candidate not found: %s", candidateKey);
        return candidate;
    }

    public CostDomain<C> costs()
    {
        return physicalModel.costs();
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Record a newly created candidate for the given requirement.
     * <p>
     * The candidate's alternative must be current in Memo and the resulting
     * candidate key must be new. This method records only the candidate layer:
     * it does not modify group optimization states or winner tiers.
     */
    public void putNewCandidate(R requirement, Candidate<C, R, P> candidate)
    {
        verifyRecent(candidate.alternativeKey());
        CandidateKey<R> candidateKey = candidateKey(candidate.alternativeKey(), requirement, physicalModel.requirements());
        checkArgument(!candidateKeyToCandidate.containsKey(candidateKey), "candidate already exists: %s", candidateKey);

        candidateKeyToCandidate.put(candidateKey, candidate);
        alternativeToCandidateKeys.computeIfAbsent(candidate.alternativeKey(), _ -> new LinkedHashSet<>()).add(candidateKey);
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Mark all parent alternatives depending on the given child request state as
     * dirty with the given reason.
     * <p>
     * The child state key must use the current group id.
     * Returns the list of affected groups, being the owning groups of the
     * marked alternatives. The returned group ids are recent.
     */
    public List<Integer> markParentAlternativesDirty(StateKey<R> childState, DirtyReason reason)
    {
        verifyRecent(childState);

        // identify dirty parent alternatives
        List<AlternativeKey<R>> dirtyAlternatives = stateToParentAlternatives.getOrDefault(childState, ImmutableSet.of()).stream()
                .toList();

        // mark alternatives as dirty with given reason
        dirtyAlternatives.forEach(alternativeKey -> this.dirtyAlternatives.computeIfAbsent(alternativeKey, _ -> new LinkedHashSet<>()).add(reason));

        // return the list of affected groups
        return dirtyAlternatives.stream()
                .map(AlternativeKey::operation)
                .map(memo::getGroupId)
                .map(Optional::orElseThrow)
                .distinct()
                .toList();
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.processChanges(...)} against current Memo state.
     * <p>
     * Return the set of state keys for the given group id.
     * The returned set is a snapshot and may be empty if the group currently
     * has no known request states.
     */
    public Set<StateKey<R>> getStateKeys(int groupId)
    {
        verifyRecent(groupId);
        return ImmutableSet.copyOf(groupToStateKeys.getOrDefault(groupId, Set.of()));
    }

    /**
     * This is a stable-state API.
     * <p>
     * Return the set of alternative keys for the given operation.
     * <p>
     * The operation must be current in Memo.
     * The returned set is a snapshot and may be empty if the operation
     * currently has no known alternatives.
     */
    public Set<AlternativeKey<R>> getAlternativeKeys(MemoOperation operation)
    {
        verifyRecent(operation);
        return ImmutableSet.copyOf(operationToAlternativeKeys.getOrDefault(operation, Set.of()));
    }

    /**
     * This is a stable-state API.
     * <p>
     * Return the set of parent alternatives depending on the given child
     * request state.
     * <p>
     * The child state key must use the current group id.
     * The returned set is a snapshot and may be empty if the child state
     * currently has no known parent alternatives.
     */
    public Set<AlternativeKey<R>> getParentAlternatives(StateKey<R> childState)
    {
        verifyRecent(childState);
        return ImmutableSet.copyOf(stateToParentAlternatives.getOrDefault(childState, Set.of()));
    }

    /**
     * This is a stable-state API.
     * It is used by {@code Pass.process(...)} against current Memo state.
     * <p>
     * Return the accumulated dirty reasons for the given alternative.
     * <p>
     * The alternative's operation must be current in Memo.
     * The returned set is a snapshot and may be empty if the alternative is
     * currently clean.
     */
    public Set<DirtyReason> getDirtyReasons(AlternativeKey<R> alternativeKey)
    {
        verifyRecent(alternativeKey);
        return ImmutableSet.copyOf(dirtyAlternatives.getOrDefault(alternativeKey, ImmutableSet.of()));
    }

    /**
     * This is a stable-state API.
     * <p>
     * Return the set of candidate keys for the given alternative.
     * <p>
     * The alternative's operation must be current in Memo.
     * The returned set is a snapshot and may be empty if the alternative
     * currently has no known candidates.
     */
    public Set<CandidateKey<R>> getCandidateKeys(AlternativeKey<R> alternativeKey)
    {
        verifyRecent(alternativeKey);
        return ImmutableSet.copyOf(alternativeToCandidateKeys.getOrDefault(alternativeKey, Set.of()));
    }

    // ** CHANGE-REPLAY API for reconciling the optimization store with the current Memo state **

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Remove all optimization state owned by the given operation.
     * <p>
     * This clears the operation's alternatives, candidates, dirty markers, and
     * child-state-to-parent-alternative links, and removes the invalidated
     * candidates from winner tiers of the owning group.
     */
    public void removeOperation(MemoOperation operation, int groupId)
    {
        // identify alternatives of the removed operation
        Set<AlternativeKey<R>> alternativesToRemove = operationToAlternativeKeys.getOrDefault(operation, ImmutableSet.of());

        // remove mappings from child states to removed alternatives
        alternativesToRemove.stream()
                .map(this::getChildStates)
                .flatMap(Set::stream)
                .distinct()
                .forEach(stateKey -> stateToParentAlternatives.computeIfPresent(stateKey, (_, parentAlternatives) -> {
                    parentAlternatives.removeAll(alternativesToRemove);
                    return parentAlternatives.isEmpty() ? null : parentAlternatives;
                }));

        // identify candidates based on the removed alternatives
        Set<CandidateKey<R>> candidatesToRemove = alternativesToRemove.stream()
                .map(alternativeKey -> alternativeToCandidateKeys.getOrDefault(alternativeKey, Set.of()))
                .flatMap(Set::stream)
                .collect(toImmutableSet());

        // clear mappings
        operationToAlternativeKeys.remove(operation);
        alternativeKeyToAlternative.keySet().removeAll(alternativesToRemove);
        candidateKeyToCandidate.keySet().removeAll(candidatesToRemove);
        alternativeToCandidateKeys.keySet().removeAll(alternativesToRemove);
        dirtyAlternatives.keySet().removeAll(alternativesToRemove);

        // remove invalidated candidates from Winners of the owning group
        groupToStateKeys.getOrDefault(groupId, ImmutableSet.of())
                .forEach(stateKey -> stateKeyToState.put(stateKey, stateKeyToState.get(stateKey).removeCandidates(candidatesToRemove)));
    }

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Remove all optimization state owned by the given group.
     * <p>
     * This clears the group's request states and all optimization state of the
     * group's operations: alternatives, candidates, dirty markers, and
     * child-state-to-parent-alternative links.
     */
    public void removeGroup(int groupId, List<MemoOperation> operations)
    {
        Set<StateKey<R>> statesToRemove = groupToStateKeys.getOrDefault(groupId, ImmutableSet.of());
        groupToStateKeys.remove(groupId);
        stateKeyToState.keySet().removeAll(statesToRemove);
        stateToParentAlternatives.keySet().removeAll(statesToRemove);

        // identify alternatives of all group's operations.
        // operations snapshot comes from the moment of group pruning. The operations weren't changed afterward, so we can assume they're recent.
        Set<AlternativeKey<R>> alternativesToRemove = operations.stream()
                .map(operation -> operationToAlternativeKeys.getOrDefault(operation, Set.of()))
                .flatMap(Set::stream)
                .collect(toImmutableSet());

        // remove mappings from child states to removed alternatives
        alternativesToRemove.stream()
                .map(this::getChildStates)
                .flatMap(Set::stream)
                .distinct()
                .forEach(stateKey -> stateToParentAlternatives.computeIfPresent(stateKey, (_, parentAlternatives) -> {
                    parentAlternatives.removeAll(alternativesToRemove);
                    return parentAlternatives.isEmpty() ? null : parentAlternatives;
                }));

        // identify candidates based on the removed alternatives
        Set<CandidateKey<R>> candidatesToRemove = alternativesToRemove.stream()
                .map(alternativeKey -> alternativeToCandidateKeys.getOrDefault(alternativeKey, Set.of()))
                .flatMap(Set::stream)
                .collect(toImmutableSet());

        // clear mappings
        operationToAlternativeKeys.keySet().removeAll(ImmutableSet.copyOf(operations));
        alternativeKeyToAlternative.keySet().removeAll(alternativesToRemove);
        candidateKeyToCandidate.keySet().removeAll(candidatesToRemove);
        alternativeToCandidateKeys.keySet().removeAll(alternativesToRemove);
        dirtyAlternatives.keySet().removeAll(alternativesToRemove);
    }

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Merge optimization states of two groups after a local Memo-level group merge.
     * <p>
     * This method only remaps and merges state keys keyed by the merged group id.
     * It does not remap alternatives or candidates for parent operations referencing
     * the merged group. Those rewrites are expected to arrive later as separate
     * operation-level events such as {@code OperationInsert}, {@code OperationSuccession},
     * {@code OperationRemove}.
     * <p>
     * This intentionally introduces temporary inconsistency during ordered batch
     * replay. After this method, parent alternatives may reference stale child groups.
     */
    public void mergeGroups(int survivingGroupId, int removedGroupId)
    {
        if (survivingGroupId == removedGroupId) {
            return;
        }

        // remove states for the removed group
        Set<StateKey<R>> removedStateKeys = groupToStateKeys.getOrDefault(removedGroupId, Set.of());
        groupToStateKeys.remove(removedGroupId);
        if (removedStateKeys.isEmpty()) {
            return;
        }

        Set<StateKey<R>> survivingStateKeys = groupToStateKeys.computeIfAbsent(survivingGroupId, _ -> new LinkedHashSet<>());

        // move the states onto the surviving group
        for (StateKey<R> removedStateKey : removedStateKeys) {
            GroupOptimizationState<C, R, P> removedState = stateKeyToState.remove(removedStateKey);
            StateKey<R> survivingStateKey = stateKey(survivingGroupId, removedStateKey.requirement(), physicalModel.requirements());
            GroupOptimizationState<C, R, P> survivingState = stateKeyToState.get(survivingStateKey);
            GroupOptimizationState<C, R, P> mergedState;
            if (survivingState == null) {
                mergedState = removedState.withGroupId(survivingGroupId);
            }
            else {
                mergedState = mergeStates(survivingState, removedState);
            }

            stateKeyToState.put(survivingStateKey, mergedState);
            survivingStateKeys.add(survivingStateKey);

            // remove mappings for the stale states
            stateToParentAlternatives.remove(removedStateKey);
        }
    }

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Populate partial optimization state for the successor operation from its predecessor.
     * <p>
     * This event is expected to follow a {@code GroupMerge} that merged child groups,
     * so that the optimization state can be inherited when operation's children are updated.
     * <p>
     * Only alternatives and candidates missing for the successor operation are inherited.
     * If the successor already has an alternative under the same key, that existing state is kept.
     * Newly inherited alternatives are marked with {@link DirtyReason#INHERITED} and should be
     * considered the best effort until the owning group is reprocessed.
     */
    public void inheritOperationState(MemoOperation oldOperation, MemoOperation newOperation, int groupId)
    {
        if (oldOperation.equals(newOperation)) {
            return;
        }

        // what the predecessor operation brings
        Set<AlternativeKey<R>> oldAlternativeKeys = operationToAlternativeKeys.getOrDefault(oldOperation, Set.of());
        if (oldAlternativeKeys.isEmpty()) {
            return;
        }

        // what state already exists for the successor operation
        Set<AlternativeKey<R>> newAlternativeKeys = operationToAlternativeKeys.computeIfAbsent(newOperation, _ -> new LinkedHashSet<>());
        Map<StateKey<R>, List<Candidate<C, R, P>>> candidatesToAddByState = new LinkedHashMap<>();

        // inherit only the missing alternatives of the old operation for the new operation
        for (AlternativeKey<R> oldAlternativeKey : oldAlternativeKeys) {
            OperationImplementationAlternative<C, R, P> oldAlternative = alternativeKeyToAlternative.get(oldAlternativeKey);
            AlternativeKey<R> newAlternativeKey = alternativeKey(newOperation, oldAlternative.childRequirements(), physicalModel.requirements());
            if (alternativeKeyToAlternative.containsKey(newAlternativeKey)) {
                continue;
            }

            OperationImplementationAlternative<C, R, P> inheritedAlternative = alternative(
                    newOperation,
                    oldAlternative.childRequirements(),
                    oldAlternative.derivedChildAttributes(),
                    oldAlternative.childCosts(),
                    oldAlternative.providedProperty(),
                    oldAlternative.cost(),
                    physicalModel.requirements(),
                    physicalModel.properties());
            alternativeKeyToAlternative.put(newAlternativeKey, inheritedAlternative);
            newAlternativeKeys.add(newAlternativeKey);

            // mark inherited state as dirty(INHERITED), preserving predecessor dirty reasons
            Set<DirtyReason> newDirtyReasons = dirtyAlternatives.computeIfAbsent(newAlternativeKey, _ -> new LinkedHashSet<>());
            newDirtyReasons.addAll(dirtyAlternatives.getOrDefault(oldAlternativeKey, Set.of()));
            newDirtyReasons.add(DirtyReason.INHERITED);

            // link child states to new alternative
            for (StateKey<R> childStateKey : getChildStates(newAlternativeKey)) {
                stateToParentAlternatives.computeIfAbsent(childStateKey, _ -> new LinkedHashSet<>()).add(newAlternativeKey);
            }

            // rekey the old alternative's candidates for the new alternative
            Set<CandidateKey<R>> oldCandidateKeys = alternativeToCandidateKeys.getOrDefault(oldAlternativeKey, Set.of());
            Set<CandidateKey<R>> newCandidateKeys = alternativeToCandidateKeys.computeIfAbsent(newAlternativeKey, _ -> new LinkedHashSet<>());
            for (CandidateKey<R> oldCandidateKey : oldCandidateKeys) {
                Candidate<C, R, P> oldCandidate = candidateKeyToCandidate.get(oldCandidateKey);
                CandidateKey<R> newCandidateKey = candidateKey(newAlternativeKey, oldCandidateKey.requirement(), physicalModel.requirements());
                Candidate<C, R, P> inheritedCandidate = new Candidate<>(inheritedAlternative, oldCandidate.enforcerPipeline(), physicalModel.costs());
                candidateKeyToCandidate.put(newCandidateKey, inheritedCandidate);
                newCandidateKeys.add(newCandidateKey);
                candidatesToAddByState.computeIfAbsent(stateKey(groupId, oldCandidateKey.requirement(), physicalModel.requirements()), _ -> new ArrayList<>()).add(inheritedCandidate);
            }
        }

        // add new candidates to states for the group
        candidatesToAddByState.forEach((stateKey, candidates) -> stateKeyToState.computeIfPresent(
                stateKey,
                (_, state) -> {
                    GroupOptimizationState<C, R, P> updatedState = state;
                    for (Candidate<C, R, P> candidate : candidates) {
                        updatedState = updatedState.addCandidate(candidate);
                    }
                    return updatedState;
                }));
    }

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Mark all alternatives referencing given group as a child as dirty with the given reason.
     * <p>
     * Returns the list of affected groups, being the owning groups of the marked alternatives.
     * The returned group Ids are obtained from Memo, so they are recent.
     */
    public List<Integer> markParentAlternativesDirty(int groupId, DirtyReason reason)
    {
        // identify dirty parent alternatives
        List<AlternativeKey<R>> dirtyAlternatives = groupToStateKeys.getOrDefault(groupId, ImmutableSet.of()).stream()
                .map(stateKey -> stateToParentAlternatives.getOrDefault(stateKey, ImmutableSet.of()))
                .flatMap(Set::stream)
                .toList();

        // mark alternatives as dirty with given reason
        dirtyAlternatives.forEach(alternativeKey -> this.dirtyAlternatives.computeIfAbsent(alternativeKey, _ -> new LinkedHashSet<>()).add(reason));

        // return the list of affected groups
        return dirtyAlternatives.stream()
                .map(AlternativeKey::operation)
                .map(memo::getGroupId)
                // operation might have been removed later in the update batch
                .filter(Optional::isPresent)
                .map(Optional::get)
                .distinct()
                .toList();
    }

    /**
     * This is a change-replay API.
     * It is used while replaying ordered {@code MemoChange} batches.
     * Temporary inconsistency is allowed.
     * <p>
     * Mark all alternatives rooted in the given group
     * as dirty with the given reason. If the group was removed later in the
     * update batch, there are no current alternatives to mark.
     */
    public void markGroupAlternativesDirty(int groupId, DirtyReason reason)
    {
        MemoGroup group = memo.getGroup(memo.getRecentId(groupId));
        if (group == null) {
            return;
        }
        group.operations().stream()
                .map(operation -> operationToAlternativeKeys.getOrDefault(operation, Set.of()))
                .flatMap(Set::stream)
                .forEach(alternativeKey -> dirtyAlternatives.computeIfAbsent(alternativeKey, _ -> new LinkedHashSet<>()).add(reason));
    }

    private Set<StateKey<R>> getChildStates(AlternativeKey<R> alternativeKey)
    {
        Set<StateKey<R>> childStates = new LinkedHashSet<>();
        List<Child> children = alternativeKey.operation().children();
        List<R> childRequirements = alternativeKey.childRequirements();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof GroupChild groupChild) {
                childStates.add(stateKey(groupChild.groupId(), childRequirements.get(i), physicalModel.requirements()));
            }
        }
        return childStates;
    }

    private void verifyRecent(StateKey<R> state)
    {
        checkArgument(state.groupId() == memo.getRecentId(state.groupId()), "groupId is stale");
    }

    private void verifyRecent(AlternativeKey<R> alternativeKey)
    {
        checkArgument(alternativeKey.operation().equals(memo.getRecentVersion(alternativeKey.operation())), "operation is stale");
    }

    private void verifyRecent(MemoOperation operation)
    {
        checkArgument(operation.equals(memo.getRecentVersion(operation)), "operation is stale");
    }

    private void verifyRecent(int groupId)
    {
        checkArgument(groupId == memo.getRecentId(groupId), "groupId is stale");
    }
}
