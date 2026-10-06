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
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Program;
import io.trino.sql.planner.exploratory.Candidate.CandidateKey;
import io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupAttributesChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupMerge;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupPrune;
import io.trino.sql.planner.exploratory.MemoChangesCollector.MemoChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationAttributesChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationInsert;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationRemove;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationSuccession;
import io.trino.sql.planner.exploratory.MemoChangesCollector.SingletonGroupCreate;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.RuleApplicationContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.planner.exploratory.Candidate.CandidateKey.candidateKey;
import static io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey.stateKey;
import static io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey.alternativeKey;
import static io.trino.sql.planner.exploratory.OperationImplementationAlternative.alternative;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_ATTRIBUTES_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.CHILD_COST_CHANGED;
import static io.trino.sql.planner.exploratory.OptimizationStore.DirtyReason.GROUP_ATTRIBUTES_CHANGED;
import static io.trino.sql.planner.exploratory.ReuseUtils.getReusedOperations;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.decomposeRequirement;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.getCost;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.getEnforcers;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.getProperty;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

public class Pass<C extends Cost, R extends Requirement, P extends Property>
{
    private final Memo memo;
    private final RuleApplier ruleApplier;
    // TODO the rule set should include rule priorities and suppressions, not just a list. Model this.
    // TODO we should be able to pre-select rules for an operation, for example by root type, not try to match all rules to each operation
    private final List<OperationRule> rules;
    private final OptimizationStore<C, R, P> optimizationStore;
    private final R rootRequirement;
    private final PhysicalModel<C, R, P> physicalModel;

    // TODO cost boundaries (max, min) and cost-based pruning of search space
    // TODO limit the size of the cache (see a TODO in RuleApplicationCache)
    // TODO add hard exploration and materialization limits (ENG-27912).
    //  The rule application cache bounds repeats of one context, not the number of
    //  distinct contexts, so it is not a termination mechanism.
    private final RuleApplicationCache appliedRules = new RuleApplicationCache();

    // TODO Consider smarter state queue ordering.
    private final Deque<StateKey<R>> statesQueue = new ArrayDeque<>();
    private final Set<StateKey<R>> queuedStates = new HashSet<>();

    private Pass(
            Program program,
            List<OperationRule> rules,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        requireNonNull(program, "program is null");
        requireNonNull(dialectRegistry, "dialectRegistry is null");
        checkArgument(getReusedOperations(program).isEmpty(), "XO does not support programs with reuse yet");
        this.memo = Memo.forProgram(program, dialectRegistry);
        this.ruleApplier = new RuleApplier(memo);
        this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        this.physicalModel = requireNonNull(physicalModel, "physicalModel is null");
        this.optimizationStore = new OptimizationStore<>(memo, physicalModel);
        // rootRequirement is canonical by the RequirementDomain contract
        this.rootRequirement = physicalModel.requirements().rootRequirement();
    }

    public static <C extends Cost, R extends Requirement, P extends Property> Pass<C, R, P> initialize(
            Program program,
            List<OperationRule> rules,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return new Pass<>(program, rules, dialectRegistry, physicalModel);
    }

    public Program run()
    {
        StateKey<R> rootKey = stateKey(memo.rootGroup(), rootRequirement, physicalModel.requirements());
        GroupOptimizationState<C, R, P> rootState = GroupOptimizationState.<C, R, P>initialize(rootKey, physicalModel.costs());
        optimizationStore.putNewState(rootState);
        enqueue(rootState);

        while (!statesQueue.isEmpty()) {
            // TODO we might want to finish early even if there are still enqueued states. We only have to make sure that each state has at least one candidate.
            dequeueAndRecent().ifPresent(this::process);
        }

        validateStatesAtBestKnownCost();

        // extract the optimized program
        // TODO fail with XO_ERROR when a reachable state has no winner (ENG-27912).
        //  Extraction assumes every reachable (Group, Requirement) has at least one
        //  candidate, and takes the first winner without checking.
        ProgramExtractor<C, R, P> programExtractor = new ProgramExtractor<>(memo, rootRequirement, optimizationStore);
        return programExtractor.extractProgram();
    }

    private void validateStatesAtBestKnownCost()
    {
        for (int groupId : memo.groups().keySet()) {
            for (StateKey<R> stateKey : optimizationStore.getStateKeys(groupId)) {
                GroupOptimizationState<C, R, P> state = optimizationStore.getState(stateKey);
                Optional<C> bestKnownCost = state.bestKnownCost();
                if (bestKnownCost.isEmpty()) {
                    continue;
                }

                Optional<C> winnerCost = state.winnerCost();
                Map<AlternativeKey<R>, Set<OptimizationStore.DirtyReason>> dirtyAlternatives = dirtyAlternatives(groupId);
                checkState(
                        winnerCost.isPresent(),
                        "State %s has best known cost %s but no current winner; dirty alternatives: %s",
                        stateKey,
                        bestKnownCost.orElseThrow(),
                        dirtyAlternatives);
                checkState(
                        physicalModel.costs().compare(winnerCost.orElseThrow(), bestKnownCost.orElseThrow()) == EQUAL,
                        "State %s current winner cost %s does not match best known cost %s; dirty alternatives: %s",
                        stateKey,
                        winnerCost.orElseThrow(),
                        bestKnownCost.orElseThrow(),
                        dirtyAlternatives);
            }
        }
    }

    private Map<AlternativeKey<R>, Set<OptimizationStore.DirtyReason>> dirtyAlternatives(int groupId)
    {
        Map<AlternativeKey<R>, Set<OptimizationStore.DirtyReason>> dirtyAlternatives = new LinkedHashMap<>();
        for (MemoOperation operation : memo.getGroup(groupId).operations()) {
            for (AlternativeKey<R> alternativeKey : optimizationStore.getAlternativeKeys(operation)) {
                Set<OptimizationStore.DirtyReason> dirtyReasons = optimizationStore.getDirtyReasons(alternativeKey);
                if (!dirtyReasons.isEmpty()) {
                    dirtyAlternatives.put(alternativeKey, dirtyReasons);
                }
            }
        }
        return dirtyAlternatives;
    }

    /**
     * Enqueue a group optimization state for processing.
     * Group id must be recent, and the state must already be present.
     */
    private void enqueue(GroupOptimizationState<C, R, P> state)
    {
        StateKey<R> key = state.key();
        verifyRecent(key);
        checkState(optimizationStore.containsState(key), "state must be already present");

        // do not enqueue duplicates
        // Note: duplicates might still be queued by different group id if group id changes
        if (queuedStates.add(key)) {
            statesQueue.add(key);
        }
    }

    /**
     * Dequeue the next non-deprecated group optimization state for processing.
     * <p>
     * Deprecated queued states are discarded until a valid recent state is found.
     * The returned state always uses the recent group id and is present in the
     * optimization store at the time of return.
     * <p>
     * Returns {@code Optional.empty()} only after the queue is exhausted without
     * finding any further valid state.
     */
    private Optional<GroupOptimizationState<C, R, P>> dequeueAndRecent()
    {
        while (!statesQueue.isEmpty()) {
            StateKey<R> key = statesQueue.remove();
            queuedStates.remove(key);

            StateKey<R> recentKey = recentStateKey(key);
            if (optimizationStore.containsState(recentKey)) {
                GroupOptimizationState<C, R, P> state = optimizationStore.getState(recentKey);
                return Optional.of(state);
            }
        }
        return Optional.empty();
    }

    private void verifyRecent(StateKey<R> state)
    {
        checkState(state.groupId() == memo.getRecentId(state.groupId()), "groupId is stale");
    }

    private StateKey<R> recentStateKey(StateKey<R> key)
    {
        return stateKey(memo.getRecentId(key.groupId()), key.requirement(), physicalModel.requirements());
    }

    private boolean isTerminalOperation(MemoOperation operation)
    {
        return TRUE.equals(operation.attributes().get(new AttributeKey(IR, TERMINAL)));
    }

    /**
     * Refresh a state with all currently available knowledge:
     * <ul>
     *     <li>explore all operations in the group with all rules, unless already applied.</li>
     *     <li>optimize all eligible operations with all requirement decompositions,
     *     reusing clean alternatives and candidates already present in the store.</li>
     *     <li>refresh any missing or dirty implementation alternative before using it.</li>
     *     <li>derive actual winners for the state.</li>
     * </ul>
     * If a child request is not yet processed, it is not processed recursively.
     * It is enqueued for future processing and INFINITE cost is assumed.
     * Replacing a dirty alternative also invalidates its stored candidates, so
     * they are recreated on demand while rebuilding the winner tier of the
     * current state and other affected states.
     */
    private void process(GroupOptimizationState<C, R, P> state)
    {
        verifyRecent(state.key());

        // explore the state exhaustively. Newly added operations or operations inherited from merged groups will be explored in a loop.
        // the need for re-processing is indicated by the boolean returned from processChanges().
        // Note: we assume that the currently processed state cannot be removed by pruning, so we can safely re-process it.
        // search policy: restart after change
        // rule application can remove or succeed operations, merge groups, and change
        // RuleApplicationContext. Restart from the recent group after every change affecting
        // this state so rules only inspect current operations with freshly computed contexts.
        boolean processCurrentState = true;
        boolean pruningRequired = false;
        while (processCurrentState) {
            processCurrentState = false;
            StateKey<R> recentKey = recentStateKey(state.key());
            // explore all operations with all rules. Operations and rules explored earlier will be skipped
            // update states according to ChangesCollector after each rule application
            List<MemoOperation> operations = memo.getGroup(recentKey.groupId()).operations();
            // TODO when exploration becomes stale because group attributes changed in the meantime, and we need to re-apply rule with new knowledge, that should be indicated by RuleApplicationContext
            for (MemoOperation operation : operations) {
                for (OperationRule rule : rules) { // TODO index rules and pre-select them by root operation type
                    RuleApplicationContext applicationContext = rule.getApplicationContext(operation, memo);
                    if (appliedRules.markIfNew(operation, rule, applicationContext)) {
                        List<MemoChange> changes = ruleApplier.applyRule(operation, rule);
                        pruningRequired |= changes.stream().anyMatch(Pass::mayCreateOrphanedGroups);
                        processCurrentState = processChanges(changes, recentStateKey(state.key()));
                        if (processCurrentState) {
                            break;
                        }
                    }
                }
                if (processCurrentState) {
                    break;
                }
            }
        }

        // get recent group key in case it was updated during rule application
        StateKey<R> recentKey = recentStateKey(state.key());

        if (pruningRequired) {
            // prune unreachable groups after exploration is completed
            List<MemoChange> changes = memo.pruneOrphanedGroups();
            processChanges(changes, recentKey);
            checkState(optimizationStore.containsState(recentKey), "currently processed state was removed by orphaned-group pruning: %s", recentKey);
        }

        GroupOptimizationState<C, R, P> baselineState = optimizationStore.getState(recentKey);

        // choose "physical" operations to implement and cost. Initially, all operations present in the group after exploration.
        MemoGroup group = memo.getGroup(recentKey.groupId());
        List<MemoOperation> operationsToImplement = group.operations();
        Attributes groupAttributes = group.attributes();

        R requirement = recentKey.requirement();

        // rebuild Winners for State
        WinnersTier<C, R, P> newWinners = WinnersTier.empty(physicalModel.costs());
        Set<AlternativeKey<R>> processedAlternativeKeys = new HashSet<>();

        for (MemoOperation operation : operationsToImplement) {
            List<List<R>> requirementDecompositionAlternatives = decomposeRequirement(operation, requirement, memo.dialectRegistry(), physicalModel);
            // process all requirement decompositions
            for (List<R> requirementAlternative : requirementDecompositionAlternatives) {
                checkState(
                        requirementAlternative.size() == operation.children().size(),
                        "Requirement decomposition size (%s) does not match operation children size (%s)",
                        requirementAlternative.size(),
                        operation.children().size());
                AlternativeKey<R> alternativeKey = alternativeKey(operation, requirementAlternative, physicalModel.requirements());
                // different requirement decompositions might have been canonicalized to the same. skip duplicates
                if (!processedAlternativeKeys.add(alternativeKey)) {
                    continue;
                }
                // canonicalized
                List<R> childRequirements = alternativeKey.childRequirements();

                Optional<OperationImplementationAlternative<C, R, P>> existingAlternative = optimizationStore.findAlternative(alternativeKey);
                boolean alternativeDirty = existingAlternative.isPresent() && !optimizationStore.getDirtyReasons(alternativeKey).isEmpty();
                OperationImplementationAlternative<C, R, P> alternative;

                // create or refresh the alternative if needed.
                if (existingAlternative.isEmpty() || alternativeDirty) {
                    Optional<C> previousCost = existingAlternative.map(OperationImplementationAlternative::cost);

                    // collect child costs and enqueue child states that haven't been processed yet
                    List<Attributes> derivedChildAttributes = new ArrayList<>();
                    List<P> childProperties = new ArrayList<>();
                    List<C> childCosts = new ArrayList<>();
                    for (int i = 0; i < operation.children().size(); i++) {
                        Child child = operation.children().get(i);
                        R childRequirement = childRequirements.get(i);
                        switch (child) {
                            case GroupChild groupChild -> {
                                derivedChildAttributes.add(memo.getGroup(groupChild.groupId()).attributes());
                                // A decomposition fixes the property promised by each child requirement.
                                // Use that promise rather than an incidental stronger property of the current
                                // child winner. Stronger properties are explored through other decompositions.
                                childProperties.add(physicalModel.properties().minimalSatisfying(childRequirement));
                                StateKey<R> childStateKey = stateKey(groupChild.groupId(), childRequirement, physicalModel.requirements());
                                if (optimizationStore.containsState(childStateKey)) {
                                    GroupOptimizationState<C, R, P> childState = optimizationStore.getState(childStateKey);
                                    // child state already processed, use the best cost established by relaxation
                                    childCosts.add(childState.bestKnownCost().orElse(physicalModel.costs().infinite()));
                                }
                                else {
                                    // child state not yet processed, enqueue it with the child requirement and assume infinite cost for now
                                    GroupOptimizationState<C, R, P> newChildState = GroupOptimizationState.<C, R, P>initialize(childStateKey, physicalModel.costs());
                                    optimizationStore.putNewState(newChildState);
                                    enqueue(newChildState);
                                    childCosts.add(physicalModel.costs().infinite());
                                }
                            }
                            case ParameterChild _ -> {
                                derivedChildAttributes.add(DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
                                childProperties.add(physicalModel.properties().scalar());
                                childCosts.add(physicalModel.costs().parameter());
                            }
                        }
                    }
                    OperationImplementationAlternative<C, R, P> newAlternative = alternative(
                            operation,
                            childRequirements,
                            derivedChildAttributes,
                            childCosts,
                            getProperty(operation, groupAttributes, derivedChildAttributes, childProperties, memo.dialectRegistry(), physicalModel),
                            getCost(operation, groupAttributes, derivedChildAttributes, childProperties, childCosts, previousCost, memo.dialectRegistry(), physicalModel),
                            physicalModel.requirements(),
                            physicalModel.properties());

                    // register the new alternative
                    if (existingAlternative.isEmpty()) {
                        optimizationStore.putNewAlternative(newAlternative);
                    }
                    else {
                        // while replacing the alternative, all candidates based on the old alternative are removed
                        optimizationStore.replaceAlternative(newAlternative);
                    }
                    alternative = newAlternative;
                }
                else {
                    alternative = existingAlternative.orElseThrow();
                }

                // Compose the alternative with EnforcerPipeline ==> candidate
                CandidateKey<R> candidateKey = candidateKey(alternativeKey, requirement, physicalModel.requirements());
                Optional<Candidate<C, R, P>> existingCandidate = optimizationStore.findCandidate(candidateKey);
                if (existingCandidate.isEmpty()) {
                    EnforcerPipeline<C, R, P> enforcerPipeline = getEnforcers(
                            operation.dialect(),
                            operation.resultType(),
                            groupAttributes,
                            alternative.providedProperty(),
                            requirement,
                            memo.dialectRegistry(),
                            physicalModel);
                    checkState(!isTerminalOperation(operation) || enforcerPipeline.steps().isEmpty(), "Terminal operation must not require enforcers above itself: %s", operation.operationId().name());

                    Candidate<C, R, P> candidate = new Candidate<>(alternative, enforcerPipeline, physicalModel.costs());
                    optimizationStore.putNewCandidate(requirement, candidate);
                    newWinners.add(candidate);
                }
                else {
                    newWinners.add(existingCandidate.orElseThrow());
                }
            }
        }

        // update GroupOptimizationState with new Winners
        GroupOptimizationState<C, R, P> newState = baselineState.replaceWinners(newWinners);
        boolean notifyParents = newState.shouldNotifyParents();
        if (notifyParents) {
            newState = newState.withParentsNotified();
        }
        optimizationStore.replaceState(newState);

        if (notifyParents) {
            // The winner-cost presence changed or its cost left the tier recorded at the last parent notification.
            // Mark all dependent parent alternatives dirty and enqueue their states for reprocessing.
            // This also covers the self-revisit case when the current state is its own parent.
            for (int groupId : optimizationStore.markParentAlternativesDirty(recentKey, CHILD_COST_CHANGED)) {
                for (StateKey<R> stateKey : optimizationStore.getStateKeys(groupId)) {
                    // TODO we can preemptively re-compute results for the affected alternatives and enqueue their groups only if the result changes (either cost or provided property (recompute EnforcerPipeline!) or exposed attributes)
                    // TODO we can also pass the information what is the reason for enqueueing so that we don't re-process the whole group if just one child changed.
                    enqueue(optimizationStore.getState(stateKey));
                }
            }
        }
    }

    private static boolean mayCreateOrphanedGroups(MemoChange change)
    {
        return switch (change) {
            case OperationRemove _ -> true;
            case SingletonGroupCreate _,
                 GroupMerge _,
                 GroupPrune _,
                 GroupAttributesChange _,
                 OperationAttributesChange _,
                 OperationInsert _,
                 OperationSuccession _ -> false;
        };
    }

    /**
     * Reconcile the OptimizationStore with Memo by replaying a batch of changes.
     * Enqueue affected states for re-processing except for the currently processed state.
     * If the currently processed state needs re-processing, it is indicated by `true` returned value.
     * The enqueued groups are deduplicated and canonicalized to the recent id.
     *
     * @param changes batch of Memo changes.
     * @param currentKey the key of the currently processed state, used to determine if it needs to be reprocessed.
     */
    private boolean processChanges(List<MemoChange> changes, StateKey<R> currentKey)
    {
        verifyRecent(currentKey);
        if (changes.isEmpty()) {
            return false;
        }

        Set<Integer> recentGroupsToUpdate = processChangesInternal(changes);

        // filter out groups that no longer exist
        recentGroupsToUpdate.removeIf(groupId -> memo.getGroup(groupId) == null);

        boolean reprocessCurrentState = false;
        for (int groupId : recentGroupsToUpdate) {
            for (StateKey<R> stateKey : optimizationStore.getStateKeys(groupId)) {
                if (stateKey.equals(currentKey)) {
                    reprocessCurrentState = true;
                }
                else {
                    enqueue(optimizationStore.getState(stateKey));
                }
            }
        }
        return reprocessCurrentState;
    }

    Set<Integer> processChangesInternal(List<MemoChange> changes)
    {
        // Collect canonicalized, deduplicated group Ids to update.
        Set<Integer> recentGroupsToUpdate = new LinkedHashSet<>();

        // TODO instead of processing events one by one, we could compact them.
        //  For example, if a group is modified, and then pruned, we could skip modifying the state for the group.
        //  Currently, we consume the events in order as it is the safe way to transition the OptimizationStore
        //  from the old state to the new state coherent with Memo.
        // TODO enqueue parent states when a child group gains new operations.
        //  This is needed to unblock pull-up rules and other rules whose
        //  applicability is discovered in bottom-up succession.
        //  This cannot be decided from individual OperationInsert or GroupMerge events alone:
        //  inserts can be neutralized by later removals or successions, and merges can add no
        //  effective operations. It requires a batch-level view of the final operation sets.
        for (MemoChange change : changes) {
            switch (change) {
                case SingletonGroupCreate _ -> {
                    // NOOP: No state exists for a newly created group until some request reaches it.
                }
                case GroupMerge(int survivingGroupId, int removedGroupId) -> {
                    // Remap and merge optimization states of the merged groups.
                    // Note: Parent-operation rewrites caused by child-group remapping
                    // are represented explicitly by later operation-level events.
                    // This intentionally introduces temporary inconsistency during
                    // ordered batch replay.
                    optimizationStore.mergeGroups(survivingGroupId, removedGroupId);
                    // Enqueue the surviving group for processing.
                    // If attributes of the resulting group differ from attributes of either component group, it is handled by related GroupAttributesChange event.
                    recentGroupsToUpdate.add(memo.getRecentId(survivingGroupId));
                }
                case GroupPrune(int groupId, List<MemoOperation> operations) -> {
                    // The group was no more transformed after removal, so the groupId and the operations list are final.
                    checkArgument(groupId == memo.getRecentId(groupId), "groupId is stale");
                    // Remove all data related to the group.
                    optimizationStore.removeGroup(groupId, operations);
                    // Pruned group is unreachable, so there are no affected groups to enqueue for processing.
                    // Do not process the removed group, even if it was enqueued due to earlier events.
                    recentGroupsToUpdate.remove(groupId);
                }
                case GroupAttributesChange(int groupId) -> {
                    // Enqueue the group for processing. After changing attributes, some rules might be re-applied.
                    recentGroupsToUpdate.add(memo.getRecentId(groupId));
                    // Group attributes contribute to the cost of alternatives rooted in the group.
                    optimizationStore.markGroupAlternativesDirty(groupId, GROUP_ATTRIBUTES_CHANGED);
                    // Mark all parent alternatives as dirty(CHILD_ATTRIBUTES_CHANGED)
                    List<Integer> affectedParentGroups = optimizationStore.markParentAlternativesDirty(groupId, CHILD_ATTRIBUTES_CHANGED);
                    // Enqueue all affected parent groups for processing. Note: the returned group Ids are recent.
                    recentGroupsToUpdate.addAll(affectedParentGroups);
                }
                case OperationInsert(MemoOperation _, int groupId) -> {
                    // Enqueue the owning group for processing.
                    // If attributes of the group changed due to new operation, it is handled by related GroupAttributesChange event.
                    recentGroupsToUpdate.add(memo.getRecentId(groupId));
                }
                case OperationSuccession(MemoOperation oldOperation, MemoOperation newOperation, int groupId) -> {
                    // Populate optimization state for the successor operation from its predecessor.
                    // The moved state is marked dirty(INHERITED).
                    // Currently, we only propagate optimization state when operation is updated after child group merge.
                    // The optimization state of the old operation version can be used as best effort for the new version,
                    // but it might be outdated since merged child groups may now provide lower-cost implementations.
                    optimizationStore.inheritOperationState(oldOperation, newOperation, groupId);
                    // Enqueue the owning group for processing.
                    recentGroupsToUpdate.add(memo.getRecentId(groupId));
                }
                case OperationAttributesChange _ -> {
                    // NOOP. We only care if group attributes change. Rule application looks at unified group attributes.
                }
                case OperationRemove(MemoOperation operation, int groupId) -> {
                    // Enqueue the owning group for processing.
                    recentGroupsToUpdate.add(memo.getRecentId(groupId));
                    // Remove all related state
                    optimizationStore.removeOperation(operation, groupId);
                }
            }
        }
        return recentGroupsToUpdate;
    }
}
