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
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MemoOperationMatcher
{
    private final Optional<String> dialect;
    private final Optional<String> name;
    private final Optional<List<Type>> argumentTypes;
    private final Optional<List<Type>> regionTypes;
    private final Optional<Type> resultType;
    private final Optional<List<Type>> groupParameterTypes;
    private final Optional<List<ChildMatcher>> children;
    private final Optional<Attributes> attributes;

    private MemoOperationMatcher(
            Optional<String> dialect,
            Optional<String> name,
            Optional<List<Type>> argumentTypes,
            Optional<List<Type>> regionTypes,
            Optional<Type> resultType,
            Optional<List<Type>> groupParameterTypes,
            Optional<List<ChildMatcher>> children,
            Optional<Attributes> attributes)
    {
        this.dialect = requireNonNull(dialect, "dialect is null");
        this.name = requireNonNull(name, "name is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
        this.regionTypes = requireNonNull(regionTypes, "regionTypes is null");
        this.resultType = requireNonNull(resultType, "resultType is null");
        this.groupParameterTypes = requireNonNull(groupParameterTypes, "groupParameterTypes is null");
        this.children = requireNonNull(children, "children is null");
        this.attributes = requireNonNull(attributes, "attributes is null");
    }

    public static MemoOperationMatcherBuilder memoOperation()
    {
        return new MemoOperationMatcherBuilder();
    }

    public static MemoOperationMatcherBuilder memoOperationLike(MemoOperation memoOperation)
    {
        return MemoOperationMatcherBuilder.like(memoOperation);
    }

    public void match(MemoOperation actual)
    {
        dialect.ifPresent(expectedDialect -> {
            if (!actual.dialect().equals(expectedDialect)) {
                throw new AssertionError("Expected dialect: " + expectedDialect + ", but found: " + actual.dialect());
            }
        });

        name.ifPresent(expectedName -> {
            if (!actual.operationId().name().equals(expectedName)) {
                throw new AssertionError("Expected operation name: " + expectedName + ", but found: " + actual.operationId().name());
            }
        });

        argumentTypes.ifPresent(expectedArgumentTypes -> {
            if (!actual.operationId().argumentTypes().equals(expectedArgumentTypes)) {
                throw new AssertionError("Expected argumentTypes: " + expectedArgumentTypes + ", but found: " + actual.operationId().argumentTypes());
            }
        });

        regionTypes.ifPresent(expectedRegionTypes -> {
            if (!actual.operationId().regionTypes().equals(expectedRegionTypes)) {
                throw new AssertionError("Expected regionTypes: " + expectedRegionTypes + ", but found: " + actual.operationId().regionTypes());
            }
        });

        resultType.ifPresent(expectedResultType -> {
            if (!actual.resultType().equals(expectedResultType)) {
                throw new AssertionError("Expected resultType: " + expectedResultType + ", but found: " + actual.resultType());
            }
        });

        groupParameterTypes.ifPresent(expectedGroupParameterTypes -> {
            if (!actual.groupParameterTypes().equals(expectedGroupParameterTypes)) {
                throw new AssertionError("Expected groupParameterTypes: " + expectedGroupParameterTypes + ", but found: " + actual.groupParameterTypes());
            }
        });

        children.ifPresent(expectedChildren -> {
            if (expectedChildren.size() != actual.children().size()) {
                throw new AssertionError("Expected " + expectedChildren.size() + " children, but found: " + actual.children().size());
            }
            for (int i = 0; i < expectedChildren.size(); i++) {
                expectedChildren.get(i).match(actual.children().get(i));
            }
        });

        attributes.ifPresent(expectedAttributes -> {
            if (!actual.attributes().equals(expectedAttributes)) {
                throw new AssertionError("Expected attributes: " + expectedAttributes + ", but found: " + actual.attributes());
            }
        });
    }

    public static class MemoOperationMatcherBuilder
    {
        private Optional<String> dialect = Optional.empty();
        private Optional<String> name = Optional.empty();
        private Optional<List<Type>> argumentTypes = Optional.empty();
        private Optional<List<Type>> regionTypes = Optional.empty();
        private Optional<Type> resultType = Optional.empty();
        private Optional<List<Type>> groupParameterTypes = Optional.empty();
        private Optional<List<ChildMatcher>> children = Optional.empty();
        private Optional<Attributes> attributes = Optional.empty();

        public static MemoOperationMatcherBuilder like(MemoOperation memoOperation)
        {
            return new MemoOperationMatcherBuilder()
                    .withDialect(memoOperation.dialect())
                    .withOperationId(memoOperation.operationId())
                    .withResultType(memoOperation.resultType())
                    .withGroupParameterTypes(memoOperation.groupParameterTypes())
                    .withChildrenList(memoOperation.children())
                    .withAttributes(memoOperation.attributes());
        }

        public MemoOperationMatcherBuilder withDialect(String dialect)
        {
            this.dialect = Optional.of(dialect);
            return this;
        }

        public MemoOperationMatcherBuilder withOperationId(OperationId operationId)
        {
            this.name = Optional.of(operationId.name());
            this.argumentTypes = Optional.of(operationId.argumentTypes());
            this.regionTypes = Optional.of(operationId.regionTypes());
            return this;
        }

        public MemoOperationMatcherBuilder withName(String name)
        {
            this.name = Optional.of(name);
            return this;
        }

        public MemoOperationMatcherBuilder withResultType(Type resultType)
        {
            this.resultType = Optional.of(resultType);
            return this;
        }

        public MemoOperationMatcherBuilder withGroupParameterTypes(Type... groupParameterTypes)
        {
            return withGroupParameterTypes(ImmutableList.copyOf(groupParameterTypes));
        }

        public MemoOperationMatcherBuilder withGroupParameterTypes(List<Type> groupParameterTypes)
        {
            this.groupParameterTypes = Optional.of(groupParameterTypes);
            return this;
        }

        public MemoOperationMatcherBuilder withChildren(ChildMatcher... children)
        {
            return withChildren(ImmutableList.copyOf(children));
        }

        public MemoOperationMatcherBuilder withChildren(List<ChildMatcher> children)
        {
            this.children = Optional.of(children);
            return this;
        }

        public MemoOperationMatcherBuilder withChildrenList(List<Child> children)
        {
            this.children = Optional.of(children.stream()
                    .map(ChildMatcher::like)
                    .collect(toImmutableList()));
            return this;
        }

        public MemoOperationMatcherBuilder withAttributes(Attributes attributes)
        {
            this.attributes = Optional.of(attributes);
            return this;
        }

        public MemoOperationMatcher build()
        {
            return new MemoOperationMatcher(dialect, name, argumentTypes, regionTypes, resultType, groupParameterTypes, children, attributes);
        }
    }

    public sealed interface ChildMatcher
            permits GroupChildMatcher, ParameterChildMatcher
    {
        static ChildMatcher like(Child child)
        {
            if (child instanceof GroupChild groupChild) {
                return GroupChildMatcher.like(groupChild);
            }
            if (child instanceof ParameterChild parameterChild) {
                return ParameterChildMatcher.like(parameterChild);
            }
            throw new IllegalArgumentException("Unknown Child type: " + child.getClass().getSimpleName());
        }

        void match(Child actual);
    }

    record GroupChildMatcher(Optional<Integer> groupId, Optional<ParameterLineage> parameterLineage)
            implements ChildMatcher
    {
        public GroupChildMatcher
        {
            requireNonNull(groupId, "groupId is null");
            requireNonNull(parameterLineage, "parameterLineage is null");
        }

        public static GroupChildMatcher groupChild()
        {
            return new GroupChildMatcher(Optional.empty(), Optional.empty());
        }

        public static GroupChildMatcher groupChild(int groupId)
        {
            return new GroupChildMatcher(Optional.of(groupId), Optional.empty());
        }

        public static GroupChildMatcher groupChild(int groupId, ParameterLineage parameterLineage)
        {
            return new GroupChildMatcher(Optional.of(groupId), Optional.of(parameterLineage));
        }

        public static GroupChildMatcher like(GroupChild groupChild)
        {
            return new GroupChildMatcher(Optional.of(groupChild.groupId()), Optional.of(groupChild.parameterLineage()));
        }

        @Override
        public void match(Child actual)
        {
            if (!(actual instanceof GroupChild(int id, ParameterLineage lineage))) {
                throw new AssertionError("Expected GroupChild, but found: " + actual.getClass().getSimpleName());
            }

            groupId.ifPresent(expectedGroupId -> {
                if (id != expectedGroupId) {
                    throw new AssertionError("Expected groupId: " + expectedGroupId + ", but found: " + id);
                }
            });

            parameterLineage.ifPresent(expectedParameterLineage -> {
                if (!lineage.equals(expectedParameterLineage)) {
                    throw new AssertionError("Expected parameterLineage: " + expectedParameterLineage + ", but found: " + lineage);
                }
            });
        }
    }

    record ParameterChildMatcher(Optional<Integer> parameterIndex)
            implements ChildMatcher
    {
        public ParameterChildMatcher
        {
            requireNonNull(parameterIndex, "parameterIndex is null");
        }

        public static ParameterChildMatcher parameterChild()
        {
            return new ParameterChildMatcher(Optional.empty());
        }

        public static ParameterChildMatcher parameterChild(int parameterIndex)
        {
            return new ParameterChildMatcher(Optional.of(parameterIndex));
        }

        public static ParameterChildMatcher like(ParameterChild parameterChild)
        {
            return new ParameterChildMatcher(Optional.of(parameterChild.groupParameterIndex()));
        }

        @Override
        public void match(Child actual)
        {
            if (!(actual instanceof ParameterChild(int index))) {
                throw new AssertionError("Expected ParameterChild, but found: " + actual.getClass().getSimpleName());
            }

            parameterIndex.ifPresent(expectedIndex -> {
                if (index != expectedIndex) {
                    throw new AssertionError("Expected parameterIndex: " + expectedIndex + ", but found: " + index);
                }
            });
        }
    }
}
