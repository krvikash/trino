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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Group-centric builder for a {@link Memo} used in rule tests.
 * <p>
 * The unit of construction is the group; a group holds one or more operations. Groups and operations are
 * referenced by {@link GroupHandle}s and {@link OperationHandle}s rather than by raw ids, and an operation's
 * child edges are described symbolically with {@link ChildSpecification}s ({@link #parameter} / {@link #childGroup}).
 * Child handles are resolved to concrete {@link GroupChild} ids only at {@link #build}, so groups may be
 * declared up front and wired into cycles or diamonds.
 * <p>
 * Typical use:
 * <pre>{@code
 * GroupHandle source = builder.declareGroup("source");
 * GroupHandle root = builder.declareGroup("root");
 * builder.addOperation(source, "leaf", leafOperation);
 * builder.addOperation(root, "project", projectOperation);   // projectOperation references childGroup(source, lineage)
 * TestingMemo testingMemo = builder.build(root);
 * }</pre>
 */
public final class TestingMemoBuilder
{
    private final DialectRegistry dialectRegistry;
    private final Map<String, GroupHandle> groupsByName = new LinkedHashMap<>();
    private final List<GroupHandle> declarationOrder = new ArrayList<>();
    private final Map<Integer, List<OperationEntry>> operationsByGroup = new LinkedHashMap<>();
    // Allocates group ids while declaring groups, then continues allocating for groups that rules create at
    // application time. The same allocator is handed to the built Memo so the id sequences never collide.
    private final Memo.IdAllocator groupIdAllocator = new Memo.IdAllocator();

    public TestingMemoBuilder(DialectRegistry dialectRegistry)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
    }

    /**
     * Reserves a group id and returns a handle for it. The group may be referenced as a child before any
     * operations are added to it, which is what allows cycles and shared sub-graphs.
     */
    public GroupHandle declareGroup(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(!groupsByName.containsKey(name), "Group already declared: %s", name);
        GroupHandle handle = new GroupHandle(name, groupIdAllocator.newId());
        groupsByName.put(name, handle);
        declarationOrder.add(handle);
        operationsByGroup.put(handle.reservedId(), new ArrayList<>());
        return handle;
    }

    /**
     * Adds an operation to a declared group. All operations in a group must share result type and group
     * parameter types (enforced by {@link MemoGroup} at build time).
     */
    public OperationHandle addOperation(GroupHandle group, String operationName, OperationSpecification specification)
    {
        requireNonNull(group, "group is null");
        requireNonNull(operationName, "operationName is null");
        requireNonNull(specification, "specification is null");
        checkArgument(groupsByName.get(group.name()) == group, "Group not declared on this builder: %s", group.name());
        OperationHandle handle = new OperationHandle(operationName, group);
        operationsByGroup.get(group.reservedId()).add(new OperationEntry(handle, specification));
        return handle;
    }

    public TestingMemo build(GroupHandle root)
    {
        requireNonNull(root, "root is null");
        checkArgument(groupsByName.get(root.name()) == root, "Root group not declared on this builder: %s", root.name());

        Map<Integer, MemoGroup> groups = new LinkedHashMap<>();
        Map<MemoOperation, Integer> operationToGroup = new LinkedHashMap<>();
        Map<String, MemoOperation> operationsByHandle = new LinkedHashMap<>();

        for (GroupHandle group : declarationOrder) {
            List<OperationEntry> entries = operationsByGroup.get(group.reservedId());
            checkState(!entries.isEmpty(), "Group has no operations: %s", group.name());

            MemoGroup memoGroup = null;
            for (OperationEntry entry : entries) {
                MemoOperation operation = entry.specification().resolve(dialectRegistry);
                operationsByHandle.put(entry.handle().name(), operation);
                operationToGroup.put(operation, group.reservedId());
                if (memoGroup == null) {
                    memoGroup = MemoGroup.singletonGroup(operation, dialectRegistry);
                }
                else {
                    memoGroup.addOperation(operation, dialectRegistry);
                }
            }
            groups.put(group.reservedId(), memoGroup);
        }

        Multimap<Integer, Integer> groupToParents = LinkedHashMultimap.create();
        groups.forEach((groupId, group) -> group.getChildGroups()
                .forEach(childGroup -> groupToParents.put(childGroup, groupId)));

        Memo memo = new Memo(root.reservedId(), groups, operationToGroup, groupToParents, dialectRegistry, groupIdAllocator);
        return new TestingMemo(memo, root, operationsByHandle);
    }

    /**
     * A child edge that points to one of the operation's group parameters.
     */
    public static ChildSpecification parameter(int parameterIndex)
    {
        return new ParameterReference(parameterIndex);
    }

    /**
     * A child edge that references another group, carrying its parameter lineage.
     */
    public static ChildSpecification childGroup(GroupHandle group, ParameterLineage lineage)
    {
        return new GroupReference(group, lineage);
    }

    public record GroupHandle(String name, int reservedId)
    {
        public GroupHandle
        {
            requireNonNull(name, "name is null");
        }
    }

    public record OperationHandle(String name, GroupHandle group)
    {
        public OperationHandle
        {
            requireNonNull(name, "name is null");
            requireNonNull(group, "group is null");
        }
    }

    public sealed interface ChildSpecification
            permits GroupReference,
                    ParameterReference {}

    public record ParameterReference(int parameterIndex)
            implements ChildSpecification {}

    public record GroupReference(GroupHandle group, ParameterLineage lineage)
            implements ChildSpecification
    {
        public GroupReference
        {
            requireNonNull(group, "group is null");
            requireNonNull(lineage, "lineage is null");
        }
    }

    private record OperationEntry(OperationHandle handle, OperationSpecification specification)
    {
        private OperationEntry
        {
            requireNonNull(handle, "handle is null");
            requireNonNull(specification, "specification is null");
        }
    }

    /**
     * Description of a single operation with symbolic child edges. Resolved to a concrete
     * {@link MemoOperation} at build time, when child group handles are turned into {@link GroupChild} ids.
     */
    public static final class OperationSpecification
    {
        private final String dialect;
        private final OperationId operationId;
        private final Type resultType;
        private List<Type> groupParameterTypes = ImmutableList.of();
        private List<ChildSpecification> children = ImmutableList.of();
        private Attributes attributes = Attributes.empty();
        // When present, build via the raw MemoOperation constructor with these inherent keys instead of
        // deriving them from the dialect registry. Needed for fake test dialects without registered metadata.
        private Optional<Set<AttributeKey>> inherentOperationAttributeKeys = Optional.empty();

        private OperationSpecification(String dialect, OperationId operationId, Type resultType)
        {
            this.dialect = requireNonNull(dialect, "dialect is null");
            this.operationId = requireNonNull(operationId, "operationId is null");
            this.resultType = requireNonNull(resultType, "resultType is null");
        }

        public static OperationSpecification operation(String dialect, OperationId operationId, Type resultType)
        {
            return new OperationSpecification(dialect, operationId, resultType);
        }

        public OperationSpecification groupParameterTypes(Type... groupParameterTypes)
        {
            return groupParameterTypes(ImmutableList.copyOf(groupParameterTypes));
        }

        public OperationSpecification groupParameterTypes(List<Type> groupParameterTypes)
        {
            this.groupParameterTypes = ImmutableList.copyOf(groupParameterTypes);
            return this;
        }

        public OperationSpecification children(ChildSpecification... children)
        {
            return children(ImmutableList.copyOf(children));
        }

        public OperationSpecification children(List<ChildSpecification> children)
        {
            this.children = ImmutableList.copyOf(children);
            return this;
        }

        public OperationSpecification attributes(Attributes attributes)
        {
            this.attributes = requireNonNull(attributes, "attributes is null");
            return this;
        }

        /**
         * Builds the operation with explicit inherent-attribute keys via the raw {@link MemoOperation}
         * constructor, instead of deriving them from the dialect registry. Use for fake test dialects that have
         * no registered operation metadata.
         */
        public OperationSpecification inherentOperationAttributeKeys(Set<AttributeKey> inherentOperationAttributeKeys)
        {
            this.inherentOperationAttributeKeys = Optional.of(ImmutableSet.copyOf(inherentOperationAttributeKeys));
            return this;
        }

        private MemoOperation resolve(DialectRegistry dialectRegistry)
        {
            List<Child> resolvedChildren = children.stream()
                    .map(child -> switch (child) {
                        case ParameterReference(int parameterIndex) -> (Child) new ParameterChild(parameterIndex);
                        case GroupReference(GroupHandle group, ParameterLineage lineage) -> new GroupChild(group.reservedId(), lineage);
                    })
                    .collect(toImmutableList());

            return inherentOperationAttributeKeys.map(attributeKeys -> new MemoOperation(
                            dialect,
                            operationId,
                            resultType,
                            groupParameterTypes,
                            resolvedChildren,
                            attributes,
                            attributeKeys))
                    .orElseGet(() -> MemoOperation.create(
                            dialect,
                            operationId,
                            resultType,
                            groupParameterTypes,
                            resolvedChildren,
                            attributes,
                            dialectRegistry));
        }
    }
}
