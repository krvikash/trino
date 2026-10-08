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

import io.trino.matching.Capture;
import io.trino.matching.Pattern;
import io.trino.sql.planner.exploratory.MemoChangesCollector.MemoChange;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.rule.Context;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation.ChildGroupWithParameterLineage;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Objects.requireNonNull;

public class RuleApplier
{
    private final Memo memo;

    public RuleApplier(Memo memo)
    {
        this.memo = requireNonNull(memo, "memo is null");
    }

    public List<MemoChange> applyRule(MemoOperation operation, OperationRule rule)
    {
        MemoChangesCollector changesCollector = new MemoChangesCollector();
        applyRule(operation, rule, changesCollector);
        return changesCollector.getChanges();
    }

    private void applyRule(MemoOperation operation, OperationRule rule, MemoChangesCollector changesCollector)
    {
        // Apply the rule to the most recent version of the operation. If the operation was already removed, there is nothing to do.
        MemoOperation operationRecent = memo.getRecentVersion(operation);
        if (memo.getGroupId(operationRecent).isEmpty()) {
            return;
        }

        Capture<MemoOperation> rootCapture = Capture.newCapture();
        Pattern<MemoOperation> pattern = rule.pattern().capturedAs(rootCapture);
        Context ruleContext = memo.getRuleContext();

        // Collect matches first, then update Memo, so matching never observes a partially modified Memo.
        List<Result<NewMemoOperation>> results = pattern.match(operationRecent, ruleContext)
                .map(match -> {
                    MemoOperation rootOperation = match.capture(rootCapture);
                    checkState(rootOperation == operationRecent, "Expected root operation to be the same as the matched operation");
                    return rule.apply(rootOperation, match.captures(), ruleContext);
                })
                .filter(Result::isPresent)
                .collect(toImmutableList());

        // Validate all results before updating Memo, so an invalid result never leaves Memo partially modified.
        for (Result<NewMemoOperation> result : results) {
            checkAcyclic(result.get(), rule, newIdentityHashSet(), newIdentityHashSet());
        }

        List<MemoOperation> resultOperations = new ArrayList<>(results.size());
        for (Result<NewMemoOperation> result : results) {
            resultOperations.add(insertOperationRuleResult(result.get(), operationRecent, changesCollector));
        }

        if (rule.canReplaceSource()) {
            // Result operations can be equal to the source operation, modulo derived attributes, or can converge through group merges.
            // Remove the source only when at least one resulting operation is distinct from the current source.
            MemoOperation recentRoot = memo.getRecentVersion(operationRecent);
            if (resultOperations.stream()
                    .map(memo::getRecentVersion)
                    .anyMatch(resultOperation -> !resultOperation.equals(recentRoot))) {
                memo.removeOperation(recentRoot, changesCollector);
            }
        }
    }

    /**
     * Verify that the NewMemoOperation graph returned by the rule has no cycles. Shared acyclic sub-graphs are allowed.
     * Operations are compared by identity.
     */
    private static void checkAcyclic(NewMemoOperation operation, OperationRule rule, Set<NewMemoOperation> inProgress, Set<NewMemoOperation> validated)
    {
        if (validated.contains(operation)) {
            return;
        }
        checkState(inProgress.add(operation), "Rule %s produced a cyclic NewMemoOperation graph", rule.getClass().getName());
        for (Object child : operation.children()) {
            if (child instanceof ChildGroupWithParameterLineage childGroup) {
                for (NewMemoOperation childOperation : childGroup.childOperations()) {
                    checkAcyclic(childOperation, rule, inProgress, validated);
                }
            }
        }
        inProgress.remove(operation);
        validated.add(operation);
    }

    /**
     * Insert the new operation resulting from applying the rule, and merge its group with the root operation's group.
     */
    private MemoOperation insertOperationRuleResult(NewMemoOperation newMemoOperation, MemoOperation rootOperation, MemoChangesCollector changesCollector)
    {
        MemoOperation newOperation = getOperation(newMemoOperation, changesCollector);
        int insertedGroupId = memo.insertOperation(newOperation, changesCollector);
        // root operation may have changed due to merges in getOperation(), so get the recent ID
        memo.mergeGroups(memo.getGroupId(memo.getRecentVersion(rootOperation)).orElseThrow(), insertedGroupId, changesCollector);

        return newOperation;
    }

    /**
     * Create a MemoOperation from the NewMemoOperation returned by the rule, recursively creating any new child operations and child groups.
     * All child operations and child groups are inserted into Memo. The returned operation has up-to-date child group IDs,
     * and is ready to be inserted into Memo.
     * Note: recursive insertion of children might cause changes in the Memo due to group merges. Any references to operations or group IDs
     * should be updated accordingly through memo.getRecentVersion(), and memo.getRecentId().
     */
    private MemoOperation getOperation(NewMemoOperation newMemoOperation, MemoChangesCollector changesCollector)
    {
        List<Child> children = newMemoOperation.children().stream()
                .map(object -> {
                    if (object instanceof ChildGroupWithParameterLineage(List<NewMemoOperation> operations, ParameterLineage lineage)) {
                        MemoOperation childOperation = getOperation(operations.getFirst(), changesCollector);
                        int childGroupId = memo.insertOperation(childOperation, changesCollector);
                        for (int i = 1; i < operations.size(); i++) {
                            childOperation = getOperation(operations.get(i), changesCollector);
                            int nextChildGroupId = memo.insertOperation(childOperation, changesCollector);
                            // childGroupId may have changed due to merges in getOperation(), so get the recent ID
                            childGroupId = memo.mergeGroups(memo.getRecentId(childGroupId), nextChildGroupId, changesCollector);
                        }
                        return new GroupChild(childGroupId, lineage);
                    }
                    if (object instanceof Child child) {
                        return child;
                    }
                    throw new IllegalStateException("Unexpected child type: " + object.getClass().getSimpleName());
                })
                .collect(toImmutableList());

        MemoOperation operation = MemoOperation.create(
                newMemoOperation.dialect(),
                newMemoOperation.operationId(),
                newMemoOperation.resultType(),
                newMemoOperation.groupParameterTypes(),
                children,
                newMemoOperation.attributes(),
                memo.dialectRegistry());

        // While inserting child groups, some of them could have been merged into other groups
        // so we need to update the child group IDs in the newly created operation
        return memo.updateChildren(operation);
    }
}
