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
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Reusable implementation alternative for a {@link MemoOperation}.
 * <p>
 * Single parent request can result in different implementation alternatives
 * for the same operation. This is because the parent-side requirement can be
 * decomposed in different ways into child requirements. Requirement
 * decomposition is reflected through {@code childRequirements}.
 * <p>
 * The same implementation alternative can be reused by different parent-side
 * requests, if they happen to produce the same child requests.
 * <p>
 * The OperationImplementationAlternative objects are supposed to be keyed by
 * (operation, childRequirements). The child states: attributes, properties,
 * and costs are recorded to easily capture and propagate changes.
 * Field semantics:
 * <ul>
 *     <li>{@code operation} - the implemented {@link MemoOperation}</li>
 *     <li>{@code childRequirements} - requirements passed to children as the
 *     result of requirement decomposition, aligned with
 *     {@code operation.children()}.</li>
 *     <li>{@code derivedChildAttributes} - derived attributes observed on the
 *     chosen child implementations, aligned with {@code operation.children()}.
 *     </li>
 *     <li>{@code childCosts} - costs of the chosen child implementations,
 *     aligned with {@code operation.children()}. For parameter children, the
 *     cost should be modeled as zero cost.</li>
 *     <li>{@code providedProperty} - property provided by this implementation
 *     alternative to its parent, canonicalized on construction.</li>
 *     <li>{@code cost} - cost of executing this operation and its subprogram,
 *     derived by the operation from the derived attributes of its result and
 *     the chosen child implementations, their derived attributes, their
 *     provided properties, and their costs. In this flattened model, the
 *     operation computes the cost of the whole subgraph; child costs are not
 *     assumed to combine additively by a generic rule.</li>
 * </ul>
 */
public final class OperationImplementationAlternative<C extends Cost, R extends Requirement, P extends Property>
{
    private final MemoOperation operation;
    private final List<R> childRequirements; // canonicalized
    private final List<Attributes> derivedChildAttributes;
    private final List<C> childCosts;
    private final P providedProperty; // canonicalized
    private final C cost;

    public static <C extends Cost, R extends Requirement, P extends Property> OperationImplementationAlternative<C, R, P> alternative(
            MemoOperation operation,
            List<R> childRequirements,
            List<Attributes> derivedChildAttributes,
            List<C> childCosts,
            P providedProperty,
            C cost,
            RequirementDomain<R> requirements,
            PropertyDomain<R, P> properties)
    {
        requireNonNull(requirements, "requirements is null");
        requireNonNull(properties, "properties is null");
        return new OperationImplementationAlternative<>(
                operation,
                requireNonNull(childRequirements, "childRequirements is null").stream()
                        .map(requirements::canonicalize)
                        .collect(toImmutableList()),
                derivedChildAttributes,
                childCosts,
                properties.canonicalize(providedProperty),
                cost);
    }

    private OperationImplementationAlternative(
            MemoOperation operation,
            List<R> childRequirements,
            List<Attributes> derivedChildAttributes,
            List<C> childCosts,
            P providedProperty,
            C cost)
    {
        this.operation = requireNonNull(operation, "operation is null");
        // childRequirements are canonical; use alternative(...) for external callers
        this.childRequirements = ImmutableList.copyOf(requireNonNull(childRequirements, "childRequirements is null"));
        this.derivedChildAttributes = ImmutableList.copyOf(requireNonNull(derivedChildAttributes, "derivedChildAttributes is null"));
        this.childCosts = ImmutableList.copyOf(requireNonNull(childCosts, "childCosts is null"));
        // providedProperty is canonical; use alternative(...) for external callers
        this.providedProperty = requireNonNull(providedProperty, "providedProperty is null");
        this.cost = requireNonNull(cost, "cost is null");

        int childCount = operation.children().size();
        checkArgument(this.childRequirements.size() == childCount,
                "childRequirements size (%s) does not match operation children size (%s)",
                this.childRequirements.size(),
                childCount);
        checkArgument(this.derivedChildAttributes.size() == childCount,
                "derivedChildAttributes size (%s) does not match operation children size (%s)",
                this.derivedChildAttributes.size(),
                childCount);
        checkArgument(this.childCosts.size() == childCount,
                "childCosts size (%s) does not match operation children size (%s)",
                this.childCosts.size(),
                childCount);
    }

    public MemoOperation operation()
    {
        return operation;
    }

    public List<R> childRequirements()
    {
        return childRequirements;
    }

    public List<Attributes> derivedChildAttributes()
    {
        return derivedChildAttributes;
    }

    public List<C> childCosts()
    {
        return childCosts;
    }

    public P providedProperty()
    {
        return providedProperty;
    }

    public C cost()
    {
        return cost;
    }

    public AlternativeKey<R> key()
    {
        return new AlternativeKey<>(operation, childRequirements);
    }

    @Override
    public String toString()
    {
        return "OperationImplementationAlternative[operation=" + operation +
                ", childRequirements=" + childRequirements +
                ", derivedChildAttributes=" + derivedChildAttributes +
                ", childCosts=" + childCosts +
                ", providedProperty=" + providedProperty +
                ", cost=" + cost + ']';
    }

    public static final class AlternativeKey<R extends Requirement>
    {
        private final MemoOperation operation;
        private final List<R> childRequirements; // canonicalized

        public static <R extends Requirement> AlternativeKey<R> alternativeKey(
                MemoOperation operation,
                List<R> childRequirements,
                RequirementDomain<R> requirements)
        {
            requireNonNull(requirements, "requirements is null");
            return new AlternativeKey<>(
                    operation,
                    requireNonNull(childRequirements, "childRequirements is null").stream()
                            .map(requirements::canonicalize)
                            .collect(toImmutableList()));
        }

        private AlternativeKey(MemoOperation operation, List<R> childRequirements)
        {
            this.operation = requireNonNull(operation, "operation is null");
            // childRequirements are canonical; use alternativeKey(...) for external callers
            this.childRequirements = ImmutableList.copyOf(requireNonNull(childRequirements, "childRequirements is null"));
        }

        public MemoOperation operation()
        {
            return operation;
        }

        public List<R> childRequirements()
        {
            return childRequirements;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof AlternativeKey<?> otherAlternativeKey)) {
                return false;
            }
            return operation.equals(otherAlternativeKey.operation) &&
                    childRequirements.equals(otherAlternativeKey.childRequirements);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operation, childRequirements);
        }

        @Override
        public String toString()
        {
            return "AlternativeKey[operation=" + operation + ", childRequirements=" + childRequirements + ']';
        }
    }
}
