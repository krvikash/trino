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
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Declarative matcher for the shape of a {@link Memo}, designed for the directed, possibly cyclic graph
 * that the exploratory optimizer works on.
 * <p>
 * Groups are referenced by name (a {@link GroupHandle} contributes its name, and additionally pins the
 * expected group to its resolved id, asserting identity). The walk starts at the root and binds each name to
 * the actual group id it first resolves to; a name seen again must resolve to the same id, and an id must
 * map to a single name. Because already-visited groups are not re-expanded, cycles and shared sub-graphs
 * terminate and id renumbering across rule application is normalized away.
 * <p>
 * Example:
 * <pre>{@code
 * memo(root)
 *     .group(root, operationPattern().name(PROJECT).children(groupEdge(source), parameterEdge(0)))
 *     .group(source, operationPattern().name("source"))
 *     .matches(testingMemo);
 * }</pre>
 */
public final class MemoMatcher
{
    private final GroupKey root;
    private final Map<String, RegisteredGroup> patterns = new LinkedHashMap<>();
    private final List<GroupHandle> absentGroups = new ArrayList<>();
    private final List<List<GroupHandle>> sameGroupAssertions = new ArrayList<>();

    private MemoMatcher(GroupKey root)
    {
        this.root = requireNonNull(root, "root is null");
    }

    public static MemoMatcher memo(GroupHandle root)
    {
        return new MemoMatcher(GroupKey.of(root));
    }

    public static MemoMatcher memo(String root)
    {
        return new MemoMatcher(GroupKey.of(root));
    }

    public MemoMatcher group(GroupHandle key, OperationPattern... operations)
    {
        return group(GroupKey.of(key), groupPattern().operations(operations));
    }

    public MemoMatcher group(String key, OperationPattern... operations)
    {
        return group(GroupKey.of(key), groupPattern().operations(operations));
    }

    public MemoMatcher group(GroupHandle key, GroupPattern pattern)
    {
        return group(GroupKey.of(key), pattern);
    }

    public MemoMatcher group(String key, GroupPattern pattern)
    {
        return group(GroupKey.of(key), pattern);
    }

    private MemoMatcher group(GroupKey key, GroupPattern pattern)
    {
        checkArgument(!patterns.containsKey(key.name()), "Group pattern already defined: %s", key.name());
        patterns.put(key.name(), new RegisteredGroup(key, pattern));
        return this;
    }

    /**
     * Asserts that the group originally declared by {@code handle} is not reachable from the root after the rule
     * ran (e.g. its parent operation was replaced and the group left orphaned, or it was merged away). Note the
     * group may still linger in the Memo unpruned; this only asserts it is no longer part of the plan.
     */
    public MemoMatcher absentGroup(GroupHandle handle)
    {
        absentGroups.add(requireNonNull(handle, "handle is null"));
        return this;
    }

    /**
     * Asserts the given handles all resolve to the same live group, i.e. their groups were merged into one.
     */
    public MemoMatcher sameGroup(GroupHandle first, GroupHandle... rest)
    {
        List<GroupHandle> handles = new ArrayList<>();
        handles.add(requireNonNull(first, "first is null"));
        for (GroupHandle handle : rest) {
            handles.add(requireNonNull(handle, "handle is null"));
        }
        checkArgument(handles.size() >= 2, "sameGroup requires at least two handles");
        sameGroupAssertions.add(ImmutableList.copyOf(handles));
        return this;
    }

    public void matches(TestingMemo testingMemo)
    {
        requireNonNull(testingMemo, "testingMemo is null");
        Memo memo = testingMemo.memo();
        Map<String, Integer> nameToId = new HashMap<>();
        Map<Integer, String> idToName = new HashMap<>();
        Set<Integer> expanded = new HashSet<>();
        matchGroup(root.name(), memo.rootGroup(), testingMemo, nameToId, idToName, expanded);
        verifyAllPatternsReached(nameToId);
        verifyAbsentGroups(testingMemo);
        verifySameGroups(testingMemo);
    }

    private void verifyAllPatternsReached(Map<String, Integer> nameToId)
    {
        List<String> unreached = new ArrayList<>();
        for (String name : patterns.keySet()) {
            if (!nameToId.containsKey(name)) {
                unreached.add(name);
            }
        }
        if (!unreached.isEmpty()) {
            throw new AssertionError("Group patterns declared but never reached from the root: " + unreached);
        }
    }

    private void verifyAbsentGroups(TestingMemo testingMemo)
    {
        if (absentGroups.isEmpty()) {
            return;
        }
        Set<Integer> reachable = new HashSet<>(testingMemo.memo().reachablePostOrder());
        for (GroupHandle handle : absentGroups) {
            int actualId = testingMemo.groupId(handle);
            if (reachable.contains(actualId)) {
                throw new AssertionError(format(
                        "Expected group %s (id %s) to be absent, but it is reachable from the root",
                        handle.name(),
                        actualId));
            }
        }
    }

    private void verifySameGroups(TestingMemo testingMemo)
    {
        for (List<GroupHandle> handles : sameGroupAssertions) {
            GroupHandle first = handles.getFirst();
            int firstId = testingMemo.groupId(first);
            for (GroupHandle other : handles.subList(1, handles.size())) {
                int otherId = testingMemo.groupId(other);
                if (otherId != firstId) {
                    throw new AssertionError(format(
                            "Expected groups %s and %s to be merged into one, but they resolve to ids %s and %s",
                            first.name(),
                            other.name(),
                            firstId,
                            otherId));
                }
            }
            if (testingMemo.memo().getGroup(firstId) == null) {
                throw new AssertionError(format(
                        "Expected the merged group of %s to be present, but id %s is not a live group",
                        first.name(),
                        firstId));
            }
        }
    }

    private void matchGroup(String name, int actualId, TestingMemo testingMemo, Map<String, Integer> nameToId, Map<Integer, String> idToName, Set<Integer> expanded)
    {
        Integer boundId = nameToId.get(name);
        if (boundId != null) {
            if (boundId != actualId) {
                throw new AssertionError(format("Group %s expected at id %s but also reached at id %s", name, boundId, actualId));
            }
        }
        else {
            String boundName = idToName.get(actualId);
            if (boundName != null) {
                throw new AssertionError(format("Group id %s matched by both %s and %s", actualId, boundName, name));
            }
            nameToId.put(name, actualId);
            idToName.put(actualId, name);
        }

        RegisteredGroup registered = patterns.get(name);
        if (registered == null) {
            throw new AssertionError(format("No pattern defined for group %s reached at id %s", name, actualId));
        }
        GroupPattern pattern = registered.pattern();
        registered.key().handle().ifPresent(handle ->
                checkIdentity(name, handle, actualId, testingMemo));

        if (!expanded.add(actualId)) {
            return;
        }

        MemoGroup group = testingMemo.memo().getGroup(actualId);
        pattern.resultType().ifPresent(expected -> assertEqual("result type", name, expected, group.resultType()));
        pattern.groupParameterTypes().ifPresent(expected -> assertEqual("group parameter types", name, expected, group.groupParameterTypes()));
        pattern.attributes().ifPresent(expected -> assertEqual("group attributes", name, expected, group.attributes()));
        List<MemoOperation> actualOperations = group.operations();
        if (pattern.containsOnly()) {
            matchContainedOperations(name, pattern.operations(), actualOperations, testingMemo, nameToId, idToName, expanded);
        }
        else {
            if (actualOperations.size() != pattern.operations().size()) {
                throw new AssertionError(format(
                        "Group %s (id %s) expected %s operations but found %s",
                        name,
                        actualId,
                        pattern.operations().size(),
                        actualOperations.size()));
            }
            for (int i = 0; i < actualOperations.size(); i++) {
                matchOperation(name, pattern.operations().get(i), actualOperations.get(i), testingMemo, nameToId, idToName, expanded);
            }
        }
    }

    /**
     * Matches each expected pattern against some distinct actual operation, ignoring order and extra operations.
     * A pattern is tried against each not-yet-consumed operation; matching is greedy (first success wins), and
     * failed attempts are rolled back so they leave no binding side effects.
     */
    private void matchContainedOperations(String name, List<OperationPattern> expectedOperations, List<MemoOperation> actualOperations, TestingMemo testingMemo, Map<String, Integer> nameToId, Map<Integer, String> idToName, Set<Integer> expanded)
    {
        boolean[] consumed = new boolean[actualOperations.size()];
        for (int expectedIndex = 0; expectedIndex < expectedOperations.size(); expectedIndex++) {
            OperationPattern expected = expectedOperations.get(expectedIndex);
            boolean matched = false;
            for (int i = 0; i < actualOperations.size(); i++) {
                if (consumed[i]) {
                    continue;
                }
                if (tryMatchOperation(name, expected, actualOperations.get(i), testingMemo, nameToId, idToName, expanded)) {
                    consumed[i] = true;
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new AssertionError(format(
                        "Group %s has no unmatched operation matching contained operation #%s",
                        name,
                        expectedIndex));
            }
        }
    }

    private boolean tryMatchOperation(String name, OperationPattern expected, MemoOperation actual, TestingMemo testingMemo, Map<String, Integer> nameToId, Map<Integer, String> idToName, Set<Integer> expanded)
    {
        Map<String, Integer> savedNameToId = new HashMap<>(nameToId);
        Map<Integer, String> savedIdToName = new HashMap<>(idToName);
        Set<Integer> savedExpanded = new HashSet<>(expanded);
        try {
            matchOperation(name, expected, actual, testingMemo, nameToId, idToName, expanded);
            return true;
        }
        catch (AssertionError _) {
            nameToId.clear();
            nameToId.putAll(savedNameToId);
            idToName.clear();
            idToName.putAll(savedIdToName);
            expanded.clear();
            expanded.addAll(savedExpanded);
            return false;
        }
    }

    private static void checkIdentity(String name, GroupHandle handle, int actualId, TestingMemo testingMemo)
    {
        int expectedId = testingMemo.groupId(handle);
        if (expectedId != actualId) {
            throw new AssertionError(format(
                    "Group %s pinned to handle %s expected id %s but found %s",
                    name,
                    handle.name(),
                    expectedId,
                    actualId));
        }
    }

    private void matchOperation(String groupName, OperationPattern pattern, MemoOperation actual, TestingMemo testingMemo, Map<String, Integer> nameToId, Map<Integer, String> idToName, Set<Integer> expanded)
    {
        pattern.dialect().ifPresent(expected -> assertEqual("dialect", groupName, expected, actual.dialect()));
        pattern.name().ifPresent(expected -> assertEqual("operation name", groupName, expected, actual.operationId().name()));
        pattern.resultType().ifPresent(expected -> assertEqual("result type", groupName, expected, actual.resultType()));
        pattern.groupParameterTypes().ifPresent(expected -> assertEqual("group parameter types", groupName, expected, actual.groupParameterTypes()));
        pattern.attributes().ifPresent(expected -> assertEqual("attributes", groupName, expected, actual.attributes()));
        pattern.containsAttributes().ifPresent(expected -> assertContainsAttributes(groupName, expected, actual.attributes()));
        pattern.inherentAttributes().ifPresent(expected -> assertEqual("inherent attributes", groupName, expected, actual.inherentAttributes()));
        pattern.derivedAttributes().ifPresent(expected -> assertEqual("derived attributes", groupName, expected, derivedAttributes(actual)));

        pattern.children().ifPresent(expectedChildren -> {
            if (expectedChildren.size() != actual.children().size()) {
                throw new AssertionError(format(
                        "Operation in group %s expected %s children but found %s",
                        groupName,
                        expectedChildren.size(),
                        actual.children().size()));
            }
            for (int i = 0; i < expectedChildren.size(); i++) {
                matchChild(groupName, expectedChildren.get(i), actual.children().get(i), testingMemo, nameToId, idToName, expanded);
            }
        });
    }

    private void matchChild(String groupName, ChildEdge edge, Child actual, TestingMemo testingMemo, Map<String, Integer> nameToId, Map<Integer, String> idToName, Set<Integer> expanded)
    {
        switch (edge) {
            case ParameterEdge(int expectedIndex) -> {
                if (!(actual instanceof ParameterChild(int index))) {
                    throw new AssertionError(format("Expected parameter child in group %s but found %s", groupName, actual));
                }
                if (index != expectedIndex) {
                    throw new AssertionError(format("Expected parameter index %s in group %s but found %s", expectedIndex, groupName, index));
                }
            }
            case GroupEdge(GroupKey target, Optional<ParameterLineage> lineage) -> {
                if (!(actual instanceof GroupChild(int childGroupId, ParameterLineage actualLineage))) {
                    throw new AssertionError(format("Expected group child in group %s but found %s", groupName, actual));
                }
                lineage.ifPresent(expected -> assertEqual("parameter lineage", groupName, expected, actualLineage));
                matchGroup(target.name(), childGroupId, testingMemo, nameToId, idToName, expanded);
            }
        }
    }

    private static void assertEqual(String what, String groupName, Object expected, Object actual)
    {
        if (!expected.equals(actual)) {
            throw new AssertionError(format("Expected %s %s in group %s but found %s", what, expected, groupName, actual));
        }
    }

    /**
     * Asserts every expected attribute is present with the expected value; extra attributes are ignored.
     */
    private static void assertContainsAttributes(String groupName, Attributes expected, Attributes actual)
    {
        for (Map.Entry<AttributeKey, Object> entry : expected.entrySet()) {
            Object actualValue = actual.get(entry.getKey());
            if (!Objects.equals(actualValue, entry.getValue())) {
                throw new AssertionError(format(
                        "Expected attribute %s=%s in group %s but found %s",
                        entry.getKey(),
                        entry.getValue(),
                        groupName,
                        actualValue));
            }
        }
    }

    private static Attributes derivedAttributes(MemoOperation operation)
    {
        return operation.attributes().filterKeys(key -> !operation.inherentOperationAttributeKeys().contains(key));
    }

    public static GroupPattern groupPattern()
    {
        return new GroupPattern();
    }

    public static OperationPattern operationPattern()
    {
        return new OperationPattern();
    }

    public static ChildEdge parameterEdge(int parameterIndex)
    {
        return new ParameterEdge(parameterIndex);
    }

    public static ChildEdge groupEdge(GroupHandle target)
    {
        return new GroupEdge(GroupKey.of(target), Optional.empty());
    }

    public static ChildEdge groupEdge(String target)
    {
        return new GroupEdge(GroupKey.of(target), Optional.empty());
    }

    public static ChildEdge groupEdge(GroupHandle target, ParameterLineage lineage)
    {
        return new GroupEdge(GroupKey.of(target), Optional.of(lineage));
    }

    public static ChildEdge groupEdge(String target, ParameterLineage lineage)
    {
        return new GroupEdge(GroupKey.of(target), Optional.of(lineage));
    }

    private record GroupKey(String name, Optional<GroupHandle> handle)
    {
        private GroupKey
        {
            requireNonNull(name, "name is null");
            requireNonNull(handle, "handle is null");
        }

        static GroupKey of(GroupHandle handle)
        {
            return new GroupKey(handle.name(), Optional.of(handle));
        }

        static GroupKey of(String name)
        {
            return new GroupKey(name, Optional.empty());
        }
    }

    private record RegisteredGroup(GroupKey key, GroupPattern pattern)
    {
        private RegisteredGroup
        {
            requireNonNull(key, "key is null");
            requireNonNull(pattern, "pattern is null");
        }
    }

    public static final class GroupPattern
    {
        private Optional<Type> resultType = Optional.empty();
        private Optional<List<Type>> groupParameterTypes = Optional.empty();
        private Optional<Attributes> attributes = Optional.empty();
        private List<OperationPattern> operations = ImmutableList.of();
        // When true, the operations are matched as a subset (each must match some distinct operation in the
        // group, order and count are not asserted); when false, they are matched positionally and exactly.
        private boolean containsOnly;

        private GroupPattern() {}

        public GroupPattern resultType(Type resultType)
        {
            this.resultType = Optional.of(resultType);
            return this;
        }

        public GroupPattern groupParameterTypes(Type... groupParameterTypes)
        {
            this.groupParameterTypes = Optional.of(ImmutableList.copyOf(groupParameterTypes));
            return this;
        }

        public GroupPattern attributes(Attributes attributes)
        {
            this.attributes = Optional.of(attributes);
            return this;
        }

        /**
         * Asserts the group holds exactly these operations, in this order.
         */
        public GroupPattern operations(OperationPattern... operations)
        {
            this.operations = ImmutableList.copyOf(operations);
            this.containsOnly = false;
            return this;
        }

        /**
         * Asserts the group holds at least these operations (each matching a distinct operation), ignoring order
         * and any additional operations. Useful for groups that carry alternatives, e.g. after a speculative rule.
         */
        public GroupPattern containsOperations(OperationPattern... operations)
        {
            this.operations = ImmutableList.copyOf(operations);
            this.containsOnly = true;
            return this;
        }

        private Optional<Type> resultType()
        {
            return resultType;
        }

        private Optional<List<Type>> groupParameterTypes()
        {
            return groupParameterTypes;
        }

        private Optional<Attributes> attributes()
        {
            return attributes;
        }

        private List<OperationPattern> operations()
        {
            return operations;
        }

        private boolean containsOnly()
        {
            return containsOnly;
        }
    }

    public static final class OperationPattern
    {
        private Optional<String> dialect = Optional.empty();
        private Optional<String> name = Optional.empty();
        private Optional<Type> resultType = Optional.empty();
        private Optional<List<Type>> groupParameterTypes = Optional.empty();
        private Optional<Attributes> attributes = Optional.empty();
        private Optional<Attributes> containsAttributes = Optional.empty();
        private Optional<Attributes> inherentAttributes = Optional.empty();
        private Optional<Attributes> derivedAttributes = Optional.empty();
        private Optional<List<ChildEdge>> children = Optional.empty();

        private OperationPattern() {}

        public OperationPattern dialect(String dialect)
        {
            this.dialect = Optional.of(dialect);
            return this;
        }

        public OperationPattern name(String name)
        {
            this.name = Optional.of(name);
            return this;
        }

        public OperationPattern resultType(Type resultType)
        {
            this.resultType = Optional.of(resultType);
            return this;
        }

        public OperationPattern groupParameterTypes(Type... groupParameterTypes)
        {
            this.groupParameterTypes = Optional.of(ImmutableList.copyOf(groupParameterTypes));
            return this;
        }

        /**
         * Asserts the operation's full attribute map equals the given attributes.
         */
        public OperationPattern attributes(Attributes attributes)
        {
            this.attributes = Optional.of(attributes);
            return this;
        }

        /**
         * Asserts the given attributes are present with the given values, ignoring any others.
         */
        public OperationPattern containsAttributes(Attributes attributes)
        {
            this.containsAttributes = Optional.of(attributes);
            return this;
        }

        /**
         * Asserts the inherent (operation-declared) attribute subset equals the given attributes.
         */
        public OperationPattern inherentAttributes(Attributes attributes)
        {
            this.inherentAttributes = Optional.of(attributes);
            return this;
        }

        /**
         * Asserts the derived (non-inherent) attribute subset equals the given attributes.
         */
        public OperationPattern derivedAttributes(Attributes attributes)
        {
            this.derivedAttributes = Optional.of(attributes);
            return this;
        }

        public OperationPattern children(ChildEdge... children)
        {
            this.children = Optional.of(ImmutableList.copyOf(children));
            return this;
        }

        private Optional<String> dialect()
        {
            return dialect;
        }

        private Optional<String> name()
        {
            return name;
        }

        private Optional<Type> resultType()
        {
            return resultType;
        }

        private Optional<List<Type>> groupParameterTypes()
        {
            return groupParameterTypes;
        }

        private Optional<Attributes> attributes()
        {
            return attributes;
        }

        private Optional<Attributes> containsAttributes()
        {
            return containsAttributes;
        }

        private Optional<Attributes> inherentAttributes()
        {
            return inherentAttributes;
        }

        private Optional<Attributes> derivedAttributes()
        {
            return derivedAttributes;
        }

        private Optional<List<ChildEdge>> children()
        {
            return children;
        }
    }

    public sealed interface ChildEdge
            permits GroupEdge,
                    ParameterEdge {}

    private record GroupEdge(GroupKey target, Optional<ParameterLineage> lineage)
            implements ChildEdge
    {
        private GroupEdge
        {
            requireNonNull(target, "target is null");
            requireNonNull(lineage, "lineage is null");
        }
    }

    private record ParameterEdge(int parameterIndex)
            implements ChildEdge {}
}
