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
package io.trino.sql.planner.exploratory.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.Memo;
import io.trino.sql.planner.exploratory.MemoGroup;
import io.trino.sql.planner.exploratory.MemoOperation;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation.ChildGroupWithParameterLineage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.sql.planner.exploratory.rule.NewMemoOperation.operationLike;
import static io.trino.sql.planner.exploratory.rule.Patterns.children;
import static io.trino.sql.planner.exploratory.rule.Patterns.operation;
import static java.util.Objects.requireNonNull;

/**
 * An optimization rule that prunes unused parameters from child groups of an operation.
 * <p>
 * It identifies an operation which passes redundant parameters to a child group.
 * Redundant parameters are those that are not used by any operation within the referenced group:
 * either as a direct parameter child or passed further down to other groups.
 * For each such child group, the redundant parameters are removed. Child operations
 * and parameter lineage are updated accordingly.
 * <p>
 * Example:
 * <pre>
 * Operation Op1
 * └── GroupChild G1 with passed parameters [A, B][X, Y] (where A, B are group parameters, X, Y are proper parameters)
 *      └── Operation Op2
 *          ├── ParameterChild(X)
 *          └── GroupChild G2 with passed parameters [A][Z] (where A is group parameter, Z is proper parameter)
 * </pre>
 * In this example, parameters B and Y passed to group G1 are unused.
 * <p>
 * After applying the rule, we get:
 * <pre>
 * Operation Op1'
 * └── GroupChild G1' with passed parameters [A][X] (where A is group parameter, X is proper parameter)
 *      └── Operation Op2'
 *          ├── ParameterChild(X) // parameters are passed by index, so here we will pass index 1 instead of 2
 *          └── GroupChild G2 with passed parameters [A][Z] (where A is group parameter, Z is proper parameter)
 * </pre>
 * Note: The rule matches the operation and prunes all child groups in one pass.
 * It declares canReplaceSource() as true, so the RuleApplier framework can choose
 * to replace the source operation with the new one returned by this rule.
 */
public class PruneUnusedParameters
        implements OperationRule
{
    private static final Pattern<MemoOperation> PATTERN = operation().with(nonEmpty(children()));

    @Override
    public Pattern<MemoOperation> pattern()
    {
        return PATTERN;
    }

    @Override
    public boolean canReplaceSource()
    {
        return true;
    }

    /**
     * This rule depends only on which child-group parameter positions are used
     * inside each GroupChild target. We intentionally do not capture child group
     * ids or parameter lineage here, because those are already fixed by the parent
     * MemoOperation identity/equality. ParameterChild children are also skipped,
     * since this rule never prunes them directly.
     */
    @Override
    public RuleApplicationContext getApplicationContext(MemoOperation operation, Memo memo)
    {
        List<Set<Integer>> usedParametersByChildGroup = operation.children().stream()
                .filter(GroupChild.class::isInstance)
                .map(GroupChild.class::cast)
                .map(groupChild -> usedParameters(memo.getGroup(groupChild.groupId())))
                .toList();

        if (usedParametersByChildGroup.isEmpty()) {
            return RuleApplicationContext.EMPTY;
        }

        return new PruneUnusedParametersContext(usedParametersByChildGroup);
    }

    @Override
    public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
    {
        boolean changed = false;

        ImmutableList.Builder<Object> newChildren = ImmutableList.builder();
        for (Child child : operation.children()) {
            switch (child) {
                case ParameterChild _ -> newChildren.add(child);
                case GroupChild groupChild -> {
                    Optional<ChildGroupWithParameterLineage> newChild = pruneUnusedParameters(groupChild, context);
                    if (newChild.isPresent()) {
                        newChildren.add(newChild.get());
                        changed = true;
                    }
                    else {
                        newChildren.add(child);
                    }
                }
            }
        }

        if (changed) {
            return Result.of(operationLike(operation)
                    .withChildren(newChildren.build()));
        }
        return Result.unchanged();
    }

    private Optional<ChildGroupWithParameterLineage> pruneUnusedParameters(GroupChild child, Context context)
    {
        MemoGroup group = context.getGroup(child.groupId());

        if (group.groupParameterTypes().isEmpty()) {
            // nothing to prune
            return Optional.empty();
        }

        Set<Integer> allParameters = IntStream.range(0, group.groupParameterTypes().size()).boxed().collect(toImmutableSet());
        Set<Integer> usedParameters = usedParameters(group);

        Set<Integer> unusedParameters = Sets.difference(allParameters, usedParameters);
        if (unusedParameters.isEmpty()) {
            // nothing to prune
            return Optional.empty();
        }

        List<Type> newParameterTypes = new ArrayList<>();
        Map<Integer, Integer> indexRemapping = new HashMap<>();
        for (int i = 0; i < group.groupParameterTypes().size(); i++) {
            if (usedParameters.contains(i)) {
                indexRemapping.put(i, newParameterTypes.size());
                newParameterTypes.add(group.groupParameterTypes().get(i));
            }
        }

        List<NewMemoOperation> newOperations = group.operations().stream()
                .map(operation -> {
                    NewMemoOperation newOperation = operationLike(operation)
                            .withGroupParameterTypes(newParameterTypes);
                    for (int i = 0; i < operation.children().size(); i++) {
                        switch (operation.children().get(i)) {
                            case ParameterChild(int groupParameterIndex) -> {
                                int newIndex = indexRemapping.get(groupParameterIndex);
                                if (newIndex != groupParameterIndex) {
                                    newOperation.withChild(i, new ParameterChild(newIndex));
                                }
                            }
                            case GroupChild(int groupId, ParameterLineage parameterLineage) -> {
                                List<Integer> passedGroupParameters = parameterLineage.passedGroupParameters();
                                List<Integer> newPassedGroupParameters = passedGroupParameters.stream()
                                        .map(indexRemapping::get)
                                        .collect(toImmutableList());
                                if (!newPassedGroupParameters.equals(passedGroupParameters)) {
                                    newOperation.withChild(i, new GroupChild(groupId, new ParameterLineage(newPassedGroupParameters, parameterLineage.passedProperParameters())));
                                }
                            }
                        }
                    }
                    return newOperation;
                })
                .collect(toImmutableList());

        ParameterLineage inputLineage = child.parameterLineage();
        List<Integer> newPassedGroupParameters = new ArrayList<>();
        for (int i = 0; i < inputLineage.passedGroupParameters().size(); i++) {
            if (usedParameters.contains(i)) {
                newPassedGroupParameters.add(inputLineage.passedGroupParameters().get(i));
            }
        }
        List<Integer> newPassedProperParameters = new ArrayList<>();
        for (int i = 0; i < inputLineage.passedProperParameters().size(); i++) {
            if (usedParameters.contains(inputLineage.passedGroupParameters().size() + i)) {
                newPassedProperParameters.add(inputLineage.passedProperParameters().get(i));
            }
        }

        return Optional.of(new ChildGroupWithParameterLineage(newOperations, new ParameterLineage(newPassedGroupParameters, newPassedProperParameters)));
    }

    private static Set<Integer> usedParameters(MemoGroup group)
    {
        return group.operations().stream()
                .map(operation -> operation.children().stream()
                        .map(operationChild -> switch (operationChild) {
                            case ParameterChild(int groupParameterIndex) -> ImmutableList.of(groupParameterIndex);
                            case GroupChild groupChild -> groupChild.parameterLineage().passedGroupParameters();
                        })
                        .flatMap(List::stream)
                        .collect(toImmutableSet()))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
    }

    private record PruneUnusedParametersContext(List<Set<Integer>> usedParametersByChildGroup)
            implements RuleApplicationContext
    {
        private PruneUnusedParametersContext
        {
            usedParametersByChildGroup = requireNonNull(usedParametersByChildGroup, "usedParametersByChildGroup is null").stream()
                    .map(ImmutableSet::copyOf)
                    .collect(toImmutableList());
        }
    }
}
