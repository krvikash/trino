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
import io.trino.sql.newir.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.newir.FormatOptions.TESTING_PRINT_OPTIONS;
import static io.trino.sql.planner.exploratory.AttributeUtils.deriveGroupAttributes;
import static io.trino.sql.planner.exploratory.AttributeUtils.mergeGroupAttributes;
import static java.util.Objects.requireNonNull;

/**
 * MemoGroup represents a collection of logically equivalent operations stored in Memo.
 * The group can have one or more parameters that represent lambda parameters to the computation
 * represented by this group and its children.
 */
public class MemoGroup
{
    private final Type resultType;

    // Types of parameters used in the operations of this group and their children
    // All parameters referenced by the operations in this group must be listed here.
    // No outer scope references are allowed.
    private final List<Type> groupParameterTypes;

    // Logically equivalent operations
    private final List<MemoOperation> operations = new ArrayList<>();

    // Logical properties of the group, always up to date with attributes declared by operations in the group.
    // Group attributes change in a monotonic way - they can only be added/strengthened as we accumulate more knowledge.
    private Attributes attributes;

    private MemoGroup(
            Type resultType,
            List<Type> groupParameterTypes,
            List<MemoOperation> operations,
            Attributes attributes)
    {
        requireNonNull(resultType, "resultType is null");
        requireNonNull(groupParameterTypes, "groupParameterTypes is null");
        requireNonNull(operations, "operations is null");
        requireNonNull(attributes, "attributes is null");

        checkArgument(operations.stream().map(MemoOperation::resultType).allMatch(resultType::equals), "all operations must have the same result type as the group");
        checkArgument(operations.stream().map(MemoOperation::groupParameterTypes).allMatch(groupParameterTypes::equals), "all operations must have the same input parameter types as the group");

        this.resultType = resultType;
        this.groupParameterTypes = ImmutableList.copyOf(groupParameterTypes);
        this.operations.addAll(operations);
        this.attributes = attributes;
    }

    public static MemoGroup singletonGroup(MemoOperation operation, DialectRegistry dialectRegistry)
    {
        requireNonNull(operation, "operation is null");

        return new MemoGroup(
                operation.resultType(),
                operation.groupParameterTypes(),
                ImmutableList.of(operation),
                deriveGroupAttributes(Attributes.empty(), ImmutableList.of(operation.attributes()), dialectRegistry));
    }

    public Type resultType()
    {
        return resultType;
    }

    public List<Type> groupParameterTypes()
    {
        return groupParameterTypes;
    }

    public List<MemoOperation> operations()
    {
        return ImmutableList.copyOf(operations);
    }

    public Attributes attributes()
    {
        return attributes;
    }

    /**
     * Adds an operation to this group. Deduplicates based on operation equality.
     */
    public void addOperation(MemoOperation operation, DialectRegistry dialectRegistry)
    {
        boolean refreshAttributes = addOperation(operation);
        if (refreshAttributes) {
            refreshGroupAttributes(dialectRegistry);
        }
    }

    /**
     * Adds an operation to this group without refreshing group attributes.
     * Returns true if operation was added or if it replaced an existing operation, false if it was equal to an existing operation including attributes.
     * If true is returned, caller should call refreshGroupAttributes() to update group attributes with the new operation's attributes.
     */
    private boolean addOperation(MemoOperation operation)
    {
        requireNonNull(operation, "operation is null");

        int index = operations.indexOf(operation);
        if (index >= 0) {
            MemoOperation existingOperation = operations.get(index);
            if (!existingOperation.attributes().equals(operation.attributes())) {
                operations.set(index, operation);
                return true;
            }
            return false;
        }
        else {
            checkArgument(resultType.equals(operation.resultType()), "the added operation must have the same result type as the group");
            checkArgument(groupParameterTypes.equals(operation.groupParameterTypes()), "the added operation must have the same input parameter types as the group");
            operations.add(operation);
            return true;
        }
    }

    public void mergeWith(MemoGroup otherGroup, DialectRegistry dialectRegistry)
    {
        requireNonNull(otherGroup, "otherGroup is null");
        checkArgument(groupParameterTypes.equals(otherGroup.groupParameterTypes), "cannot merge groups with different input parameter types");
        checkArgument(resultType.equals(otherGroup.resultType), "cannot merge groups with different result types");

        mergeAttributesWith(otherGroup.attributes(), dialectRegistry);
        for (int i = 0; i < otherGroup.operations.size(); i++) {
            addOperation(otherGroup.operations.get(i));
        }
        refreshGroupAttributes(dialectRegistry);
    }

    public void removeOperation(MemoOperation operation, DialectRegistry dialectRegistry)
    {
        requireNonNull(operation, "operation is null");

        int index = operations.indexOf(operation);
        checkArgument(index >= 0, "Removed operation not found in the group");
        checkArgument(operations.size() > 1, "Cannot remove the only operation from the group");
        operations.remove(index);
        refreshGroupAttributes(dialectRegistry);
    }

    private void refreshGroupAttributes(DialectRegistry dialectRegistry)
    {
        Attributes refreshedAttributes = deriveGroupAttributes(
                attributes,
                operations.stream().map(MemoOperation::attributes).toList(),
                dialectRegistry);
        attributes = refreshedAttributes;
    }

    private void mergeAttributesWith(Attributes otherAttributes, DialectRegistry dialectRegistry)
    {
        attributes = mergeGroupAttributes(attributes, otherAttributes, dialectRegistry);
    }

    public Set<Integer> getChildGroups()
    {
        return operations().stream()
                .flatMap(operation -> operation.children().stream())
                .filter(MemoOperation.GroupChild.class::isInstance)
                .map(MemoOperation.GroupChild.class::cast)
                .map(MemoOperation.GroupChild::groupId)
                .collect(toImmutableSet());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        MemoGroup other = (MemoGroup) o;
        return Objects.equals(resultType, other.resultType) &&
                Objects.equals(groupParameterTypes, other.groupParameterTypes) &&
                Objects.equals(operations, other.operations) &&
                Objects.equals(attributes, other.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resultType, groupParameterTypes, operations, attributes);
    }

    @Override
    public String toString()
    {
        return MemoDebugPrinter.printGroup(this, TESTING_PRINT_OPTIONS);
    }
}
