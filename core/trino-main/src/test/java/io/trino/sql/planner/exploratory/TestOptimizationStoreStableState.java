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

import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.exploratory.MemoTestingHelper.ignoreChanges;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_ATTRIBUTES_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_COST_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.UNDEFINED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternative;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeWithChild;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.baseMemo;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidate;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.leafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.operationWithChild;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.replacedAlternative;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.state;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.stateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.store;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.winners;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOptimizationStoreStableState
{
    @Test
    public void testPutNewState()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        // insert a new empty state
        var stateKey = stateKey(memo.rootGroup(), UNDEFINED);
        var state = state(stateKey);
        store.putNewState(state);
        assertThat(store.getState(stateKey)).isSameAs(state);

        // insert another state with the same key
        var stateWithSameKey = state(stateKey);
        assertThatThrownBy(() -> store.putNewState(stateWithSameKey))
                .hasMessageContaining("state already exists");

        // create a non-empty state and a corresponding operation in Memo
        MemoOperation operation = leafOperation();
        int groupId = memo.insertOperation(operation, ignoreChanges());
        var nonEmptyState = state(stateKey)
                .replaceWinners(winners(candidate(alternative(operation))))
                .withGroupId(groupId);
        assertThatThrownBy(() -> store.putNewState(nonEmptyState))
                .hasMessageContaining("new state must not have winners");
    }

    @Test
    public void testContainsState()
    {
        Memo memo = baseMemo();
        var store = store(memo);
        var stateKey = stateKey(memo.rootGroup(), UNDEFINED);

        assertThat(store.containsState(stateKey)).isFalse();

        store.putNewState(state(stateKey));

        assertThat(store.containsState(stateKey)).isTrue();
    }

    @Test
    public void testGetState()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        var stateKey = stateKey(memo.rootGroup(), UNDEFINED);
        var state = state(stateKey);

        // the state is not inserted into store yet
        assertThat(store.containsState(stateKey)).isFalse();
        assertThatThrownBy(() -> store.getState(stateKey))
                .hasMessageContaining("state not found");

        // insert the state
        store.putNewState(state);
        assertThat(store.containsState(stateKey)).isTrue();
        assertThat(store.getState(stateKey)).isSameAs(state);
    }

    @Test
    public void testReplaceState()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        var stateKey = stateKey(memo.rootGroup(), UNDEFINED);
        var state = state(stateKey);

        // the state is not inserted into store yet
        assertThat(store.containsState(stateKey)).isFalse();
        assertThatThrownBy(() -> store.replaceState(state))
                .hasMessageContaining("state not found");

        // insert empty state
        store.putNewState(state);
        assertThat(store.getState(stateKey)).isSameAs(state);
        assertThat(store.getState(stateKey).winners()).hasSize(0);

        // replace the empty state with a state having one candidate
        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var replacedState = state.replaceWinners(winners(candidate(alternative(operation))));
        store.replaceState(replacedState);
        assertThat(store.getState(stateKey)).isSameAs(replacedState);
        assertThat(store.getState(stateKey).winners()).hasSize(1);
    }

    @Test
    public void testPutNewAlternative()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        // insert a new alternative with one group child
        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation operation = operationWithChild(childGroupId);
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternativeWithChild(operation);
        store.putNewAlternative(alternative);
        assertThat(store.getAlternative(alternative.key())).isSameAs(alternative);
        assertThat(store.getAlternativeKeys(alternative.operation())).containsExactly(alternative.key());
        assertThat(store.getParentAlternatives(childStateKey)).containsExactly(alternative.key());
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();

        // insert another alternative with the same key
        assertThatThrownBy(() -> store.putNewAlternative(alternative))
                .hasMessageContaining("alternative already exists");
    }

    @Test
    public void testContainsAlternative()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternative(operation);
        assertThat(store.containsAlternative(alternative.key())).isFalse();

        store.putNewAlternative(alternative);

        assertThat(store.containsAlternative(alternative.key())).isTrue();
    }

    @Test
    public void testGetAlternative()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternative(operation);

        // the alternative is not inserted into store yet
        assertThat(store.containsAlternative(alternative.key())).isFalse();
        assertThatThrownBy(() -> store.getAlternative(alternative.key()))
                .hasMessageContaining("alternative not found");

        // insert the alternative
        store.putNewAlternative(alternative);
        assertThat(store.containsAlternative(alternative.key())).isTrue();
        assertThat(store.getAlternative(alternative.key())).isSameAs(alternative);
    }

    @Test
    public void testGetAlternativeKeys()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternative(operation);

        // the operation has no known alternatives yet
        assertThat(store.getAlternativeKeys(alternative.operation())).isEmpty();

        // insert one alternative for the operation
        store.putNewAlternative(alternative);
        assertThat(store.getAlternativeKeys(alternative.operation())).containsExactly(alternative.key());
    }

    @Test
    public void testPutNewCandidate()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        // insert a new candidate
        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var candidate = candidate(alternative(operation));
        var candidateKey = candidateKey(candidate.alternativeKey(), UNDEFINED);
        store.putNewAlternative(candidate.implementation());
        store.putNewCandidate(UNDEFINED, candidate);
        assertThat(store.getCandidate(candidateKey)).isSameAs(candidate);
        assertThat(store.getCandidateKeys(candidate.alternativeKey())).containsExactly(candidateKey);

        // insert another candidate with the same key
        assertThatThrownBy(() -> store.putNewCandidate(UNDEFINED, candidate))
                .hasMessageContaining("candidate already exists");
    }

    @Test
    public void testContainsCandidate()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var candidate = candidate(alternative(operation));
        var candidateKey = candidateKey(candidate.alternativeKey(), UNDEFINED);
        assertThat(store.containsCandidate(candidateKey)).isFalse();

        store.putNewAlternative(candidate.implementation());
        store.putNewCandidate(UNDEFINED, candidate);

        assertThat(store.containsCandidate(candidateKey)).isTrue();
    }

    @Test
    public void testGetCandidate()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var candidate = candidate(alternative(operation));
        var candidateKey = candidateKey(candidate.alternativeKey(), UNDEFINED);

        // the candidate is not inserted into store yet
        assertThat(store.containsCandidate(candidateKey)).isFalse();
        assertThatThrownBy(() -> store.getCandidate(candidateKey))
                .hasMessageContaining("candidate not found");

        // insert the candidate
        store.putNewAlternative(candidate.implementation());
        store.putNewCandidate(UNDEFINED, candidate);
        assertThat(store.containsCandidate(candidateKey)).isTrue();
        assertThat(store.getCandidate(candidateKey)).isSameAs(candidate);
    }

    @Test
    public void testGetCandidateKeys()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        MemoOperation operation = leafOperation();
        memo.insertOperation(operation, ignoreChanges());
        var candidate = candidate(alternative(operation));
        var candidateKey = candidateKey(candidate.alternativeKey(), UNDEFINED);

        // the alternative has no known candidates yet
        assertThat(store.getCandidateKeys(candidate.alternativeKey())).isEmpty();

        // insert one candidate for the alternative
        store.putNewAlternative(candidate.implementation());
        store.putNewCandidate(UNDEFINED, candidate);
        assertThat(store.getCandidateKeys(candidate.alternativeKey())).containsExactly(candidateKey);
    }

    @Test
    public void testReplaceAlternative()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        // create an alternative with child, and a candidate based on it
        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation operation = operationWithChild(childGroupId);
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternativeWithChild(operation);
        var candidate = candidate(alternative);
        var candidateKey = candidateKey(candidate.alternativeKey(), UNDEFINED);
        var stateKey = stateKey(memo.getGroupId(alternative.operation()).orElseThrow(), UNDEFINED);
        var state = state(stateKey);

        // the alternative is not inserted into store yet
        assertThatThrownBy(() -> store.replaceAlternative(alternative))
                .hasMessageContaining("alternative not found");

        // insert empty state, then populate it with candidate-backed winners
        store.putNewState(state);
        store.putNewAlternative(alternative);
        store.putNewCandidate(UNDEFINED, candidate);
        store.replaceState(state.replaceWinners(winners(candidate)));
        store.markParentAlternativesDirty(childStateKey, CHILD_COST_CHANGED);
        assertThat(store.getDirtyReasons(alternative.key())).containsExactly(CHILD_COST_CHANGED);
        assertThat(store.getCandidateKeys(alternative.key())).containsExactly(candidateKey);
        assertThat(store.getState(stateKey).winners()).hasSize(1);

        // replace the alternative with the same key but a different provided property
        var replacedAlternative = replacedAlternative(alternative);
        assertThat(replacedAlternative.providedProperty()).isNotEqualTo(alternative.providedProperty());
        assertThat(replacedAlternative.key()).isEqualTo(alternative.key());

        store.replaceAlternative(replacedAlternative);

        // new alternative under the same key
        assertThat(store.getAlternative(alternative.key())).isSameAs(replacedAlternative);
        assertThat(store.getAlternative(alternative.key())).isNotSameAs(alternative);

        // mappings refer the same key
        assertThat(store.getAlternativeKeys(alternative.operation())).containsExactly(alternative.key());
        assertThat(store.getParentAlternatives(childStateKey)).containsExactly(alternative.key());

        // dirty reasons were cleared
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();

        // candidates were invalidated and removed
        assertThat(store.getCandidateKeys(alternative.key())).isEmpty();
        assertThat(store.containsCandidate(candidateKey)).isFalse();
        assertThat(store.getState(stateKey).winners()).hasSize(0);
    }

    @Test
    public void testGetStateKeys()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int groupId = memo.rootGroup();
        var stateKey = stateKey(groupId, UNDEFINED);

        // the group has no known request states yet
        assertThat(store.getStateKeys(groupId)).isEmpty();

        // insert one state for the group
        store.putNewState(state(stateKey));
        assertThat(store.getStateKeys(groupId)).containsExactly(stateKey);
    }

    @Test
    public void testMarkParentAlternativesDirty()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation operation = operationWithChild(childGroupId);
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternativeWithChild(operation);
        int parentGroupId = memo.getGroupId(alternative.operation()).orElseThrow();

        // the child state has no known parent alternatives yet
        assertThat(store.markParentAlternativesDirty(childStateKey, CHILD_COST_CHANGED)).isEmpty();

        // insert the parent alternative depending on the child state
        store.putNewAlternative(alternative);
        assertThat(store.markParentAlternativesDirty(childStateKey, CHILD_COST_CHANGED)).containsExactly(parentGroupId);
        assertThat(store.getDirtyReasons(alternative.key())).containsExactly(CHILD_COST_CHANGED);
    }

    @Test
    public void testGetParentAlternatives()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation operation = operationWithChild(childGroupId);
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternativeWithChild(operation);

        // the child state has no known parent alternatives yet
        assertThat(store.getParentAlternatives(childStateKey)).isEmpty();

        // insert one parent alternative depending on the child state
        store.putNewAlternative(alternative);
        assertThat(store.getParentAlternatives(childStateKey)).containsExactly(alternative.key());
    }

    @Test
    public void testGetDirtyReasons()
    {
        Memo memo = baseMemo();
        var store = store(memo);

        int childGroupId = memo.insertOperation(leafOperation(), ignoreChanges());
        var childStateKey = stateKey(childGroupId, UNDEFINED);
        MemoOperation operation = operationWithChild(childGroupId);
        memo.insertOperation(operation, ignoreChanges());
        var alternative = alternativeWithChild(operation);

        // the alternative is not inserted into store yet
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();

        // insert the alternative and accumulate dirty reasons
        store.putNewAlternative(alternative);
        assertThat(store.getDirtyReasons(alternative.key())).isEmpty();

        store.markParentAlternativesDirty(childStateKey, CHILD_COST_CHANGED);
        store.markParentAlternativesDirty(childStateKey, CHILD_ATTRIBUTES_CHANGED);
        assertThat(store.getDirtyReasons(alternative.key())).containsExactly(CHILD_COST_CHANGED, CHILD_ATTRIBUTES_CHANGED);
    }
}
