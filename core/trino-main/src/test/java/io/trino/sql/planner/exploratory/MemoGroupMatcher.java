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
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperationMatcher.MemoOperationMatcherBuilder;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MemoGroupMatcher
{
    private final Optional<Type> resultType;
    private final Optional<List<Type>> groupParameterTypes;
    private final Optional<List<MemoOperationMatcher>> operations;
    private final Optional<Attributes> attributes;

    private MemoGroupMatcher(
            Optional<Type> resultType,
            Optional<List<Type>> groupParameterTypes,
            Optional<List<MemoOperationMatcher>> operations,
            Optional<Attributes> attributes)
    {
        this.resultType = requireNonNull(resultType, "resultType is null");
        this.groupParameterTypes = requireNonNull(groupParameterTypes, "groupParameterTypes is null");
        this.operations = requireNonNull(operations, "operations is null");
        this.attributes = requireNonNull(attributes, "attributes is null");
    }

    public static MemoGroupMatcherBuilder memoGroup()
    {
        return new MemoGroupMatcherBuilder();
    }

    public static MemoGroupMatcherBuilder memoGroupLike(MemoGroup memoGroup)
    {
        return MemoGroupMatcherBuilder.like(memoGroup);
    }

    public void match(MemoGroup actual)
    {
        resultType.ifPresent(expected -> {
            Type actualResultType = actual.resultType();
            if (!actualResultType.equals(expected)) {
                throw new AssertionError("Expected result type " + expected + " but found " + actualResultType);
            }
        });
        groupParameterTypes.ifPresent(expected -> {
            List<Type> actualGroupParameterTypes = actual.groupParameterTypes();
            if (!actualGroupParameterTypes.equals(expected)) {
                throw new AssertionError("Expected group parameter types " + expected + " but found " + actualGroupParameterTypes);
            }
        });
        operations.ifPresent(expected -> {
            if (expected.size() != actual.operations().size()) {
                throw new AssertionError("Expected " + expected.size() + " operations but found " + actual.operations().size());
            }
            for (int i = 0; i < expected.size(); i++) {
                expected.get(i).match(actual.operations().get(i));
            }
        });
        attributes.ifPresent(expected -> {
            Attributes actualAttributes = actual.attributes();
            if (!actualAttributes.equals(expected)) {
                throw new AssertionError("Expected attributes " + expected + " but found " + actualAttributes);
            }
        });
    }

    public static class MemoGroupMatcherBuilder
    {
        private Optional<Type> resultType = Optional.empty();
        private Optional<List<Type>> groupParameterTypes = Optional.empty();
        private Optional<List<MemoOperationMatcher>> operations = Optional.empty();
        private Optional<Attributes> attributes = Optional.empty();

        public static MemoGroupMatcherBuilder like(MemoGroup memoGroup)
        {
            return new MemoGroupMatcherBuilder()
                    .withResultType(memoGroup.resultType())
                    .withGroupParameterTypes(memoGroup.groupParameterTypes())
                    .withOperations(memoGroup.operations())
                    .withAttributes(memoGroup.attributes());
        }

        public MemoGroupMatcherBuilder withResultType(Type resultType)
        {
            this.resultType = Optional.of(resultType);
            return this;
        }

        public MemoGroupMatcherBuilder withGroupParameterTypes(Type... groupParameterTypes)
        {
            return withGroupParameterTypes(ImmutableList.copyOf(groupParameterTypes));
        }

        public MemoGroupMatcherBuilder withGroupParameterTypes(List<Type> groupParameterTypes)
        {
            this.groupParameterTypes = Optional.of(groupParameterTypes);
            return this;
        }

        public MemoGroupMatcherBuilder withOperations(MemoOperationMatcher... operations)
        {
            this.operations = Optional.of(ImmutableList.copyOf(operations));
            return this;
        }

        public MemoGroupMatcherBuilder withOperations(MemoOperation... operations)
        {
            return withOperations(ImmutableList.copyOf(operations));
        }

        public MemoGroupMatcherBuilder withOperations(List<MemoOperation> operations)
        {
            this.operations = Optional.of(operations.stream()
                    .map(MemoOperationMatcher::memoOperationLike)
                    .map(MemoOperationMatcherBuilder::build)
                    .collect(toImmutableList()));
            return this;
        }

        public MemoGroupMatcherBuilder withAttributes(Attributes attributes)
        {
            this.attributes = Optional.of(attributes);
            return this;
        }

        public MemoGroupMatcher build()
        {
            return new MemoGroupMatcher(resultType, groupParameterTypes, operations, attributes);
        }
    }
}
