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
package io.trino.sql.planner.exploratory.physical;

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.EnforcerPipeline;

import java.util.List;
import java.util.Optional;

/**
 * Interprets operations in the cost, requirement, and property domains of an
 * XO physical model.
 * <p>
 * For each child, {@link #getCost} and {@link #getProperty} receive the minimal
 * property promised by its child requirement, not an incidental stronger
 * property of the selected child winner. To benefit from a stronger property,
 * requirement decomposition must request it explicitly.
 * <p>
 * Costing is monotonic. For a fixed operation and requirement decomposition,
 * improving child costs or adding derived knowledge must not increase the total
 * cost of the implementation and its enforcers. More knowledge must not weaken
 * the provided property or require more expensive enforcement.
 */
public interface XOCompanionDialect<C extends Cost, R extends Requirement, P extends Property>
{
    /**
     * Derive the cost of an operation subgraph for one concrete implementation.
     * The result depends on the particular operation being costed, its inherent
     * operation attributes, the derived attributes of its result, and the
     * particular derived attributes, properties, and costs of the chosen child
     * implementations.
     * <p>
     * For an operation in Memo, the result attributes are the attributes of its
     * group. They may therefore include logical knowledge contributed by another
     * equivalent operation. For an operation outside Memo, they are the normally
     * derived attributes of the operation.
     * <p>
     * Cost composition is tied to operation logic rather than just pure addition.
     * For that reason, this method is supposed to return the cost of the whole
     * subgraph rather than only the root operation.
     * <p>
     * {@code previousCost} is the cost previously derived for the same
     * implementation alternative. It is empty when the alternative is costed
     * for the first time or when no persistent alternative exists. The physical
     * model may use it to preserve model-owned identities across re-costing.
     */
    C getCost(
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            List<C> childCosts,
            Optional<C> previousCost);

    /**
     * Derive the property provided by an operation for one concrete
     * implementation. The result depends on the particular operation being
     * analyzed, its inherent operation attributes, the derived attributes of
     * its result, and the particular derived attributes and properties of the
     * chosen child implementations.
     * <p>
     * For an operation in Memo, the result attributes are the attributes of its
     * group. They may therefore include logical knowledge contributed by another
     * equivalent operation. For an operation outside Memo, they are the normally
     * derived attributes of the operation.
     */
    P getProperty(
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties);

    /**
     * Decompose one incoming requirement into child requirements for the given
     * operation. The result depends on the particular operation, its inherent
     * operation attributes, and the particular incoming requirement being
     * decomposed.
     * <p>
     * Terminal operations must admit only passthrough decompositions: the
     * incoming requirement must be pushed to the relevant child without
     * requiring any enforcers above the terminal operation itself. This
     * invariant is validated by the caller.
     * TODO terminal operations must use dedicated passthrough decomposition
     *  so that no enforcers are ever required above the terminal operation.
     */
    List<List<R>> decomposeRequirement(OperationId operationId, Attributes inherentOperationAttributes, R requirement);

    /**
     * Build the enforcer pipeline needed to satisfy the given requirement on
     * top of an input with the given type, derived attributes, and property.
     * The result depends on that particular input state and requirement.
     * For a Memo operation, {@code derivedBaseAttributes} must be the group
     * attributes rather than the attributes of the particular base operation.
     */
    EnforcerPipeline<C, R, P> getEnforcers(
            Type baseType,
            Attributes derivedBaseAttributes,
            P baseProperty,
            R requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel);
}
