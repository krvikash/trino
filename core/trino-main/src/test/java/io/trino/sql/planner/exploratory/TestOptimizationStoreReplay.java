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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.exploratory.MemoTestingHelper.ignoreChanges;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_ATTRIBUTES_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_COST_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.GROUP_ATTRIBUTES_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.INHERITED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.OTHER;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.UNDEFINED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternative;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeLeafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeWithChild;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.baseMemo;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidate;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.leafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.operationWithChild;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.state;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.stateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.store;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.winners;
import static org.assertj.core.api.Assertions.assertThat;

class TestOptimizationStoreReplay
{
    @Test
    public void testMergeGroupsWithNoRemovedStates()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int survivingGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        int removedGroupId = memo.insertOperation(alternativeLeafOperation(), ignoreChanges());
        var survivingStateKey = stateKey(survivingGroupId, UNDEFINED);
        var survivingState = state(survivingStateKey);

        // insert one state for the surviving group only. The removed group has no states
        store.putNewState(survivingState);

        // merge the removed group into the surviving group
        memo.mergeGroups(survivingGroupId, removedGroupId, ignoreChanges());
        store.mergeGroups(survivingGroupId, removedGroupId);

        assertThat(store.getStateKeys(survivingGroupId)).containsExactly(survivingStateKey);
        assertThat(store.getState(survivingStateKey)).isSameAs(survivingState);
    }

    @Test
    public void testMergeGroupsIntoItself()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int groupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var stateKey = stateKey(groupId, UNDEFINED);
        var state = state(stateKey);
        store.putNewState(state);

        store.mergeGroups(groupId, groupId);

        assertThat(store.getStateKeys(groupId)).containsExactly(stateKey);
        assertThat(store.getState(stateKey)).isSameAs(state);
    }

    @Test
    public void testMergeGroupsMovesStatesWithoutCounterparts()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int survivingGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        int removedGroupId = memo.insertOperation(alternativeLeafOperation(), ignoreChanges());

        var removedStateKey = stateKey(removedGroupId, UNDEFINED);
        MemoOperation winnerOperation = leafOperation();
        memo.insertOperation(winnerOperation, ignoreChanges());
        var removedState = state(removedStateKey).replaceWinners(winners(candidate(alternative(winnerOperation))));

        // insert one state for the removed group only, and add one candidate. the surviving group has no states
        store.putNewState(state(removedStateKey));
        store.replaceState(removedState);

        // merge the removed group into the surviving group
        memo.mergeGroups(survivingGroupId, removedGroupId, ignoreChanges());
        store.mergeGroups(survivingGroupId, removedGroupId);

        var movedStateKey = stateKey(survivingGroupId, UNDEFINED);
        assertThat(store.getStateKeys(survivingGroupId)).containsExactly(movedStateKey);
        assertThat(store.getState(movedStateKey).winners()).hasSize(1);
    }

    @Test
    public void testMergeGroupsMergesOverlappingStates()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation survivingOperation = leafOperation();
        int survivingGroupId = memo.insertOperation(survivingOperation, ignoreChanges());
        MemoOperation removedOperation = alternativeLeafOperation();
        int removedGroupId = memo.insertOperation(removedOperation, ignoreChanges());

        var survivingStateKey = stateKey(survivingGroupId, UNDEFINED);
        var removedStateKey = stateKey(removedGroupId, UNDEFINED);
        var survivingCandidate = candidate(alternative(survivingOperation));
        var removedCandidate = candidate(alternative(removedOperation));
        var survivingState = state(survivingStateKey).replaceWinners(winners(survivingCandidate));
        var removedState = state(removedStateKey).replaceWinners(winners(removedCandidate));

        // insert one state for each group, with the same requirement. add one candidate to each.
        store.putNewState(state(survivingStateKey));
        store.replaceState(survivingState);
        store.putNewState(state(removedStateKey));
        store.replaceState(removedState);

        // merge the removed group into the surviving group
        memo.mergeGroups(survivingGroupId, removedGroupId, ignoreChanges());
        store.mergeGroups(survivingGroupId, removedGroupId);

        // states were merged
        assertThat(store.getStateKeys(survivingGroupId)).containsExactly(survivingStateKey);
        assertThat(store.getState(survivingStateKey).winners()).hasSize(2);
    }

    @Test
    public void testMergeGroupsKeepsUnmatchedStates()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int survivingGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        int removedGroupId = memo.insertOperation(alternativeLeafOperation(), ignoreChanges());

        var survivingStateKey = stateKey(survivingGroupId, OTHER);
        var removedStateKey = stateKey(removedGroupId, UNDEFINED);

        MemoOperation survivingWinnerOperation = leafOperation();
        memo.insertOperation(survivingWinnerOperation, ignoreChanges());
        MemoOperation removedWinnerOperation = leafOperation();
        memo.insertOperation(removedWinnerOperation, ignoreChanges());
        var survivingState = state(survivingStateKey).replaceWinners(winners(candidate(alternative(survivingWinnerOperation))));
        var removedState = state(removedStateKey).replaceWinners(winners(candidate(alternative(removedWinnerOperation))));

        // insert one unmatched state for each group, and add one candidate to each
        store.putNewState(state(survivingStateKey));
        store.replaceState(survivingState);
        store.putNewState(state(removedStateKey));
        store.replaceState(removedState);

        // merge the removed group into the surviving group
        memo.mergeGroups(survivingGroupId, removedGroupId, ignoreChanges());
        store.mergeGroups(survivingGroupId, removedGroupId);

        var movedStateKey = stateKey(survivingGroupId, UNDEFINED);
        assertThat(store.getStateKeys(survivingGroupId)).containsExactlyInAnyOrder(survivingStateKey, movedStateKey);
        assertThat(store.getState(survivingStateKey).winners()).hasSize(1);
        assertThat(store.getState(movedStateKey).winners()).hasSize(1);
    }

    @Test
    public void testInheritOperationStateWithNoPredecessorState()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation predecessorOperation = leafOperation();
        memo.insertOperation(predecessorOperation, ignoreChanges());
        MemoOperation successorOperation = alternativeLeafOperation();
        int successorGroupId = memo.insertOperation(successorOperation, ignoreChanges());

        var successorStateKey = stateKey(successorGroupId, UNDEFINED);
        var successorAlternative = alternative(successorOperation);
        var successorCandidate = candidate(successorAlternative);
        var successorCandidateKey = candidateKey(successorAlternative.key(), UNDEFINED);

        // successor already has one alternative and one candidate, but predecessor has no optimization state
        store.putNewState(state(successorStateKey));
        store.putNewAlternative(successorAlternative);
        store.putNewCandidate(UNDEFINED, successorCandidate);
        store.replaceState(state(successorStateKey).replaceWinners(winners(successorCandidate)));
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternative.key());
        assertThat(store.getCandidateKeys(successorAlternative.key())).containsExactly(successorCandidateKey);
        assertThat(store.getCandidate(successorCandidateKey)).isSameAs(successorCandidate);
        assertThat(store.getDirtyReasons(successorAlternative.key())).isEmpty();
        assertThat(store.getState(successorStateKey).winners()).hasSize(1);

        // inheritance from the stateless predecessor should not change anything
        store.inheritOperationState(predecessorOperation, successorOperation, successorGroupId);
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternative.key());
        assertThat(store.getCandidateKeys(successorAlternative.key())).containsExactly(successorCandidateKey);
        assertThat(store.getCandidate(successorCandidateKey)).isSameAs(successorCandidate);
        assertThat(store.getDirtyReasons(successorAlternative.key())).isEmpty();
        assertThat(store.getState(successorStateKey).winners()).hasSize(1);
    }

    @Test
    public void testInheritOperationStateIntoItself()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        int groupId = memo.insertOperation(operation, ignoreChanges());

        var stateKey = stateKey(groupId, UNDEFINED);
        var alternative = alternative(operation);
        var candidate = candidate(alternative);
        var candidateKey = candidateKey(alternative.key(), UNDEFINED);

        // the operation already has one alternative and one candidate
        store.putNewState(state(stateKey));
        store.putNewAlternative(alternative);
        store.putNewCandidate(UNDEFINED, candidate);
        store.replaceState(state(stateKey).replaceWinners(winners(candidate)));
        assertThat(store.getAlternativeKeys(operation)).containsExactly(alternative.key());
        assertThat(store.getCandidateKeys(alternative.key())).containsExactly(candidateKey);
        assertThat(store.getCandidate(candidateKey)).isSameAs(candidate);
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();
        assertThat(store.getState(stateKey).winners()).hasSize(1);

        // inheritance into itself should not change anything
        store.inheritOperationState(operation, operation, groupId);
        assertThat(store.getAlternativeKeys(operation)).containsExactly(alternative.key());
        assertThat(store.getCandidateKeys(alternative.key())).containsExactly(candidateKey);
        assertThat(store.getCandidate(candidateKey)).isSameAs(candidate);
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();
        assertThat(store.getState(stateKey).winners()).hasSize(1);
    }

    @Test
    public void testInheritOperationStateKeepsExistingSuccessorAlternative()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation predecessorOperation = leafOperation();
        memo.insertOperation(predecessorOperation, ignoreChanges());
        MemoOperation successorOperation = alternativeLeafOperation();
        int successorGroupId = memo.insertOperation(successorOperation, ignoreChanges());

        var successorStateKey = stateKey(successorGroupId, UNDEFINED);
        var predecessorAlternative = alternative(predecessorOperation);
        var predecessorCandidate = candidate(predecessorAlternative);
        var predecessorCandidateKey = candidateKey(predecessorAlternative.key(), UNDEFINED);
        var successorAlternative = alternative(successorOperation);
        var successorCandidate = candidate(successorAlternative);
        var successorCandidateKey = candidateKey(successorAlternative.key(), UNDEFINED);

        // predecessor has one alternative and one candidate
        store.putNewAlternative(predecessorAlternative);
        store.putNewCandidate(UNDEFINED, predecessorCandidate);

        // successor already has the corresponding alternative and one candidate
        store.putNewState(state(successorStateKey));
        store.putNewAlternative(successorAlternative);
        store.putNewCandidate(UNDEFINED, successorCandidate);
        store.replaceState(state(successorStateKey).replaceWinners(winners(successorCandidate)));
        assertThat(store.getAlternativeKeys(predecessorOperation)).containsExactly(predecessorAlternative.key());
        assertThat(store.getCandidateKeys(predecessorAlternative.key())).containsExactly(predecessorCandidateKey);
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternative.key());
        assertThat(store.getCandidateKeys(successorAlternative.key())).containsExactly(successorCandidateKey);
        assertThat(store.getCandidate(successorCandidateKey)).isSameAs(successorCandidate);
        assertThat(store.getDirtyReasons(successorAlternative.key())).isEmpty();
        assertThat(store.getState(successorStateKey).winners()).hasSize(1);

        // the existing successor alternative should be kept, with no inherited candidate added
        store.inheritOperationState(predecessorOperation, successorOperation, successorGroupId);
        assertThat(store.getAlternativeKeys(predecessorOperation)).containsExactly(predecessorAlternative.key());
        assertThat(store.getCandidateKeys(predecessorAlternative.key())).containsExactly(predecessorCandidateKey);
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternative.key());
        assertThat(store.getAlternative(successorAlternative.key())).isSameAs(successorAlternative);
        assertThat(store.getCandidateKeys(successorAlternative.key())).containsExactly(successorCandidateKey);
        assertThat(store.getCandidate(successorCandidateKey)).isSameAs(successorCandidate);
        assertThat(store.getDirtyReasons(successorAlternative.key())).isEmpty();
        assertThat(store.getState(successorStateKey).winners()).hasSize(1);
    }

    @Test
    public void testInheritOperationState()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation predecessorOperation = operationWithChild("old_dialect", childGroupId);
        memo.insertOperation(predecessorOperation, ignoreChanges());
        MemoOperation successorOperation = operationWithChild("new_dialect", childGroupId);
        int successorGroupId = memo.insertOperation(successorOperation, ignoreChanges());

        var successorStateKey = stateKey(successorGroupId, UNDEFINED);
        var predecessorAlternative = alternativeWithChild(predecessorOperation);
        var successorAlternativeKey = alternativeKey(successorOperation, predecessorAlternative.childRequirements());
        var predecessorCandidate = candidate(predecessorAlternative);
        var predecessorCandidateKey = candidateKey(predecessorAlternative.key(), UNDEFINED);
        var successorCandidateKey = candidateKey(successorAlternativeKey, UNDEFINED);

        // predecessor has one alternative, one candidate and one dirty reason
        store.putNewAlternative(predecessorAlternative);
        store.putNewCandidate(UNDEFINED, predecessorCandidate);
        store.markParentAlternativesDirty(childStateKey, CHILD_COST_CHANGED);

        // successor has one empty state ready to receive inherited candidates
        store.putNewState(state(successorStateKey));

        store.inheritOperationState(predecessorOperation, successorOperation, successorGroupId);

        // predecessor-side alternative is preserved
        assertThat(store.getAlternativeKeys(predecessorOperation)).containsExactly(predecessorAlternative.key());

        // predecessor-side candidate is preserved
        assertThat(store.getCandidateKeys(predecessorAlternative.key())).containsExactly(predecessorCandidateKey);

        // predecessor-side dirty reasons are preserved
        assertThat(store.getDirtyReasons(predecessorAlternative.key())).containsExactly(CHILD_COST_CHANGED);

        // successor-side alternative is rekeyed to the successor operation
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternativeKey);

        // successor-side alternative is stored under the new key
        var inheritedAlternative = store.getAlternative(successorAlternativeKey);
        assertThat(inheritedAlternative.key()).isEqualTo(successorAlternativeKey);

        // successor-side alternative is marked dirty
        assertThat(store.getDirtyReasons(successorAlternativeKey)).containsExactlyInAnyOrder(CHILD_COST_CHANGED, INHERITED);

        // child state is linked to the new alternative in addition to the old one
        assertThat(store.getParentAlternatives(childStateKey)).containsExactlyInAnyOrder(predecessorAlternative.key(), successorAlternativeKey);

        // candidate is rekeyed to the new alternative
        assertThat(store.getCandidateKeys(successorAlternativeKey)).containsExactly(successorCandidateKey);

        // rekeyed candidate is stored and points at the inherited alternative
        var inheritedCandidate = store.getCandidate(successorCandidateKey);
        assertThat(inheritedCandidate.implementation()).isSameAs(inheritedAlternative);

        // rekeyed candidate keeps the original enforcer pipeline
        assertThat(inheritedCandidate.enforcerPipeline()).isEqualTo(predecessorCandidate.enforcerPipeline());

        // new candidate is added to the successor state's winners
        assertThat(store.getState(successorStateKey).winners()).containsExactly(inheritedCandidate);
    }

    @Test
    public void testRemoveOperation()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var undefinedChildStateKey = stateKey(childGroupId, UNDEFINED);
        var otherChildStateKey = stateKey(childGroupId, OTHER);
        MemoOperation operation = operationWithChild("test_dialect", childGroupId);
        int groupId = memo.insertOperation(operation, ignoreChanges());

        var undefinedStateKey = stateKey(groupId, UNDEFINED);
        var otherStateKey = stateKey(groupId, OTHER);
        var undefinedAlternative = alternativeWithChild(operation);
        var otherAlternative = alternativeWithChild(operation, OTHER);
        var undefinedCandidate = candidate(undefinedAlternative);
        var otherCandidate = candidate(otherAlternative);
        var undefinedCandidateKey = candidateKey(undefinedAlternative.key(), UNDEFINED);
        var otherCandidateKey = candidateKey(otherAlternative.key(), OTHER);

        // insert the child states so group-scoped dirtying can discover parent alternatives
        store.putNewState(state(undefinedChildStateKey));
        store.putNewState(state(otherChildStateKey));

        // insert two alternatives and two state-specific candidates for the same operation
        store.putNewState(state(undefinedStateKey));
        store.putNewState(state(otherStateKey));
        store.putNewAlternative(undefinedAlternative);
        store.putNewAlternative(otherAlternative);
        store.putNewCandidate(UNDEFINED, undefinedCandidate);
        store.putNewCandidate(OTHER, otherCandidate);
        store.replaceState(state(undefinedStateKey).replaceWinners(winners(undefinedCandidate)));
        store.replaceState(state(otherStateKey).replaceWinners(winners(otherCandidate)));
        store.markParentAlternativesDirty(childGroupId, CHILD_COST_CHANGED);
        assertThat(store.getAlternativeKeys(operation)).containsExactlyInAnyOrder(undefinedAlternative.key(), otherAlternative.key());
        assertThat(store.getCandidateKeys(undefinedAlternative.key())).containsExactly(undefinedCandidateKey);
        assertThat(store.getCandidateKeys(otherAlternative.key())).containsExactly(otherCandidateKey);
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getDirtyReasons(otherAlternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getParentAlternatives(undefinedChildStateKey)).containsExactly(undefinedAlternative.key());
        assertThat(store.getParentAlternatives(otherChildStateKey)).containsExactly(otherAlternative.key());
        assertThat(store.getState(undefinedStateKey).winners()).containsExactly(undefinedCandidate);
        assertThat(store.getState(otherStateKey).winners()).containsExactly(otherCandidate);

        // removing the operation clears all alternatives, candidates, dirty reasons and parent links
        store.removeOperation(operation, groupId);
        assertThat(store.getAlternativeKeys(operation)).isEmpty();
        assertThat(store.containsAlternative(undefinedAlternative.key())).isFalse();
        assertThat(store.containsAlternative(otherAlternative.key())).isFalse();
        assertThat(store.getCandidateKeys(undefinedAlternative.key())).isEmpty();
        assertThat(store.getCandidateKeys(otherAlternative.key())).isEmpty();
        assertThat(store.containsCandidate(undefinedCandidateKey)).isFalse();
        assertThat(store.containsCandidate(otherCandidateKey)).isFalse();
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).isEmpty();
        assertThat(store.getDirtyReasons(otherAlternative.key())).isEmpty();
        assertThat(store.getParentAlternatives(undefinedChildStateKey)).isEmpty();
        assertThat(store.getParentAlternatives(otherChildStateKey)).isEmpty();
        assertThat(store.getStateKeys(groupId)).containsExactlyInAnyOrder(undefinedStateKey, otherStateKey);
        assertThat(store.containsState(undefinedStateKey)).isTrue();
        assertThat(store.containsState(otherStateKey)).isTrue();
        assertThat(store.getState(undefinedStateKey).winners()).hasSize(0);
        assertThat(store.getState(otherStateKey).winners()).hasSize(0);
    }

    @Test
    public void testRemoveGroup()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var undefinedChildStateKey = stateKey(childGroupId, UNDEFINED);
        var otherChildStateKey = stateKey(childGroupId, OTHER);

        MemoOperation firstOperation = operationWithChild("test_dialect", childGroupId);
        int groupId = memo.insertOperation(firstOperation, ignoreChanges());
        MemoOperation secondOperation = operationWithChild("another_dialect", childGroupId);
        memo.mergeGroups(groupId, memo.insertOperation(secondOperation, ignoreChanges()), ignoreChanges());

        var undefinedStateKey = stateKey(groupId, UNDEFINED);
        var otherStateKey = stateKey(groupId, OTHER);
        var undefinedAlternative = alternativeWithChild(firstOperation);
        var otherAlternative = alternativeWithChild(secondOperation, OTHER);
        var undefinedCandidate = candidate(undefinedAlternative);
        var otherCandidate = candidate(otherAlternative);
        var undefinedCandidateKey = candidateKey(undefinedAlternative.key(), UNDEFINED);
        var otherCandidateKey = candidateKey(otherAlternative.key(), OTHER);

        // insert child states so parent links can be tracked
        store.putNewState(state(undefinedChildStateKey));
        store.putNewState(state(otherChildStateKey));

        // insert two request states, two alternatives, and two candidates for the pruned group
        store.putNewState(state(undefinedStateKey));
        store.putNewState(state(otherStateKey));
        store.putNewAlternative(undefinedAlternative);
        store.putNewAlternative(otherAlternative);
        store.putNewCandidate(UNDEFINED, undefinedCandidate);
        store.putNewCandidate(OTHER, otherCandidate);
        store.replaceState(state(undefinedStateKey).replaceWinners(winners(undefinedCandidate)));
        store.replaceState(state(otherStateKey).replaceWinners(winners(otherCandidate)));
        store.markParentAlternativesDirty(childGroupId, CHILD_COST_CHANGED);
        assertThat(store.getStateKeys(groupId)).containsExactlyInAnyOrder(undefinedStateKey, otherStateKey);
        assertThat(store.getAlternativeKeys(firstOperation)).containsExactly(undefinedAlternative.key());
        assertThat(store.getAlternativeKeys(secondOperation)).containsExactly(otherAlternative.key());
        assertThat(store.getCandidateKeys(undefinedAlternative.key())).containsExactly(undefinedCandidateKey);
        assertThat(store.getCandidateKeys(otherAlternative.key())).containsExactly(otherCandidateKey);
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getDirtyReasons(otherAlternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getParentAlternatives(undefinedChildStateKey)).containsExactly(undefinedAlternative.key());
        assertThat(store.getParentAlternatives(otherChildStateKey)).containsExactly(otherAlternative.key());
        assertThat(store.getState(undefinedStateKey).winners()).containsExactly(undefinedCandidate);
        assertThat(store.getState(otherStateKey).winners()).containsExactly(otherCandidate);

        // removing the group clears its states, alternatives, candidates, dirty reasons and parent links
        store.removeGroup(groupId, ImmutableList.of(firstOperation, secondOperation));
        assertThat(store.getStateKeys(groupId)).isEmpty();
        assertThat(store.containsState(undefinedStateKey)).isFalse();
        assertThat(store.containsState(otherStateKey)).isFalse();
        assertThat(store.getAlternativeKeys(firstOperation)).isEmpty();
        assertThat(store.getAlternativeKeys(secondOperation)).isEmpty();
        assertThat(store.containsAlternative(undefinedAlternative.key())).isFalse();
        assertThat(store.containsAlternative(otherAlternative.key())).isFalse();
        assertThat(store.getCandidateKeys(undefinedAlternative.key())).isEmpty();
        assertThat(store.getCandidateKeys(otherAlternative.key())).isEmpty();
        assertThat(store.containsCandidate(undefinedCandidateKey)).isFalse();
        assertThat(store.containsCandidate(otherCandidateKey)).isFalse();
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).isEmpty();
        assertThat(store.getDirtyReasons(otherAlternative.key())).isEmpty();
        assertThat(store.getParentAlternatives(undefinedChildStateKey)).isEmpty();
        assertThat(store.getParentAlternatives(otherChildStateKey)).isEmpty();

        // child states survive pruning of the parent group
        assertThat(store.getStateKeys(childGroupId)).containsExactlyInAnyOrder(undefinedChildStateKey, otherChildStateKey);
        assertThat(store.containsState(undefinedChildStateKey)).isTrue();
        assertThat(store.containsState(otherChildStateKey)).isTrue();
    }

    @Test
    public void testMarkParentAlternativesDirtyByGroup()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var undefinedChildStateKey = stateKey(childGroupId, UNDEFINED);
        var otherChildStateKey = stateKey(childGroupId, OTHER);

        MemoOperation firstParentOperation = operationWithChild("test_dialect", childGroupId);
        int firstParentGroupId = memo.insertOperation(firstParentOperation, ignoreChanges());
        MemoOperation secondParentOperation = operationWithChild("another_dialect", childGroupId);
        int secondParentGroupId = memo.insertOperation(secondParentOperation, ignoreChanges());

        var undefinedAlternative = alternativeWithChild(firstParentOperation);
        var otherAlternative = alternativeWithChild(secondParentOperation, OTHER);

        // the child group has no known request states yet
        assertThat(store.markParentAlternativesDirty(childGroupId, CHILD_COST_CHANGED)).isEmpty();

        // insert child states, but no parent alternatives yet
        store.putNewState(state(undefinedChildStateKey));
        store.putNewState(state(otherChildStateKey));
        assertThat(store.markParentAlternativesDirty(childGroupId, CHILD_COST_CHANGED)).isEmpty();

        // insert two parent alternatives referencing the child group through two different child states
        store.putNewAlternative(undefinedAlternative);
        store.putNewAlternative(otherAlternative);
        assertThat(store.getParentAlternatives(undefinedChildStateKey)).containsExactly(undefinedAlternative.key());
        assertThat(store.getParentAlternatives(otherChildStateKey)).containsExactly(otherAlternative.key());

        // dirtying by child group finds both parent groups
        assertThat(store.markParentAlternativesDirty(childGroupId, CHILD_COST_CHANGED))
                .containsExactlyInAnyOrder(firstParentGroupId, secondParentGroupId);

        // both parent alternatives receive the dirty reason
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getDirtyReasons(otherAlternative.key())).containsExactly(CHILD_COST_CHANGED);

        // dirty reasons accumulate on repeated dirtying
        assertThat(store.markParentAlternativesDirty(childGroupId, CHILD_ATTRIBUTES_CHANGED))
                .containsExactlyInAnyOrder(firstParentGroupId, secondParentGroupId);
        assertThat(store.getDirtyReasons(undefinedAlternative.key())).containsExactlyInAnyOrder(CHILD_COST_CHANGED, CHILD_ATTRIBUTES_CHANGED);
        assertThat(store.getDirtyReasons(otherAlternative.key())).containsExactlyInAnyOrder(CHILD_COST_CHANGED, CHILD_ATTRIBUTES_CHANGED);
    }

    @Test
    public void testMarkGroupAlternativesDirty()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation firstOperation = leafOperation();
        int groupId = memo.insertOperation(firstOperation, ignoreChanges());
        MemoOperation secondOperation = alternativeLeafOperation();
        memo.mergeGroups(groupId, memo.insertOperation(secondOperation, ignoreChanges()), ignoreChanges());
        MemoOperation otherOperation = operationWithChild(memo.rootGroup());
        memo.insertOperation(otherOperation, ignoreChanges());

        var firstAlternative = alternative(firstOperation);
        var secondAlternative = alternative(secondOperation);
        var otherAlternative = alternativeWithChild(otherOperation);
        store.putNewAlternative(firstAlternative);
        store.putNewAlternative(secondAlternative);
        store.putNewAlternative(otherAlternative);

        store.markGroupAlternativesDirty(groupId, GROUP_ATTRIBUTES_CHANGED);

        assertThat(store.getDirtyReasons(firstAlternative.key())).containsExactly(GROUP_ATTRIBUTES_CHANGED);
        assertThat(store.getDirtyReasons(secondAlternative.key())).containsExactly(GROUP_ATTRIBUTES_CHANGED);
        assertThat(store.getDirtyReasons(otherAlternative.key())).isEmpty();
    }

    @Test
    public void testMergeInheritRemove()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int survivingChildGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        int removedChildGroupId = memo.insertOperation(alternativeLeafOperation(), ignoreChanges());
        var survivingChildStateKey = stateKey(survivingChildGroupId, UNDEFINED);
        var removedChildStateKey = stateKey(removedChildGroupId, UNDEFINED);

        MemoOperation predecessorOperation = operationWithChild("old_dialect", removedChildGroupId);
        int predecessorGroupId = memo.insertOperation(predecessorOperation, ignoreChanges());
        MemoOperation successorOperation = operationWithChild("new_dialect", survivingChildGroupId);
        int successorGroupId = memo.insertOperation(successorOperation, ignoreChanges());

        var predecessorStateKey = stateKey(predecessorGroupId, UNDEFINED);
        var successorStateKey = stateKey(successorGroupId, UNDEFINED);
        var predecessorAlternative = alternativeWithChild(predecessorOperation);
        var successorAlternativeKey = alternativeKey(successorOperation, ImmutableList.of(UNDEFINED));
        var predecessorCandidate = candidate(predecessorAlternative);
        var predecessorCandidateKey = candidateKey(predecessorAlternative.key(), UNDEFINED);
        var successorCandidateKey = candidateKey(successorAlternativeKey, UNDEFINED);

        // insert child states and predecessor optimization state
        store.putNewState(state(survivingChildStateKey));
        store.putNewState(state(removedChildStateKey));
        store.putNewState(state(predecessorStateKey));
        store.putNewAlternative(predecessorAlternative);
        store.putNewCandidate(UNDEFINED, predecessorCandidate);
        store.replaceState(state(predecessorStateKey).replaceWinners(winners(predecessorCandidate)));
        store.markParentAlternativesDirty(removedChildStateKey, CHILD_COST_CHANGED);
        assertThat(store.getCandidateKeys(predecessorAlternative.key())).containsExactly(predecessorCandidateKey);

        // insert empty successor state
        store.putNewState(state(successorStateKey));

        // merge child groups
        memo.mergeGroups(survivingChildGroupId, removedChildGroupId, ignoreChanges());
        store.mergeGroups(survivingChildGroupId, removedChildGroupId);
        assertThat(store.getStateKeys(survivingChildGroupId)).containsExactly(survivingChildStateKey);

        // inherit predecessor state into the successor operation
        store.inheritOperationState(predecessorOperation, successorOperation, successorGroupId);
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternativeKey);
        assertThat(store.getCandidateKeys(successorAlternativeKey)).containsExactly(successorCandidateKey);
        assertThat(store.getDirtyReasons(successorAlternativeKey)).containsExactlyInAnyOrder(CHILD_COST_CHANGED, INHERITED);
        assertThat(store.getParentAlternatives(survivingChildStateKey)).containsExactly(successorAlternativeKey);
        assertThat(store.getState(successorStateKey).winners()).containsExactly(store.getCandidate(successorCandidateKey));

        // remove predecessor operation
        store.removeOperation(predecessorOperation, predecessorGroupId);

        // predecessor-side winners are removed from the owning group state
        assertThat(store.getState(predecessorStateKey).winners()).hasSize(0);

        // inherited successor-side optimization state remains intact
        assertThat(store.getAlternativeKeys(successorOperation)).containsExactly(successorAlternativeKey);
        assertThat(store.getCandidateKeys(successorAlternativeKey)).containsExactly(successorCandidateKey);
        assertThat(store.getDirtyReasons(successorAlternativeKey)).containsExactlyInAnyOrder(CHILD_COST_CHANGED, INHERITED);
        assertThat(store.getParentAlternatives(survivingChildStateKey)).containsExactly(successorAlternativeKey);
        assertThat(store.getState(successorStateKey).winners()).containsExactly(store.getCandidate(successorCandidateKey));
    }
}
