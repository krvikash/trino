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

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.exploratory.AttributeUtils.getInherentAttributes;

public class XOCompanionDialectUtils
{
    private XOCompanionDialectUtils() {}

    public static <C extends Cost, R extends Requirement, P extends Property> C getCost(
            Operation operation,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            List<C> childCosts,
            Optional<C> previousCost,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return getCost(operation.dialect(), operation.id(), getInherentAttributes(operation, dialectRegistry), derivedResultAttributes, derivedChildAttributes, childProperties, childCosts, previousCost, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> C getCost(
            MemoOperation operation,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            List<C> childCosts,
            Optional<C> previousCost,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return getCost(operation.dialect(), operation.operationId(), getInherentAttributes(operation.dialect(), operation.operationId(), operation.attributes(), dialectRegistry), derivedResultAttributes, derivedChildAttributes, childProperties, childCosts, previousCost, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> C getCost(
            String dialect,
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            List<C> childCosts,
            Optional<C> previousCost,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        XOCompanionDialect<C, R, P> companionDialect = physicalModel.companionDialect(dialectRegistry.dialect(dialect));
        return companionDialect.getCost(operationId, inherentOperationAttributes, derivedResultAttributes, derivedChildAttributes, childProperties, childCosts, previousCost);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> P getProperty(
            Operation operation,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return getProperty(operation.dialect(), operation.id(), getInherentAttributes(operation, dialectRegistry), derivedResultAttributes, derivedChildAttributes, childProperties, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> P getProperty(
            MemoOperation operation,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return getProperty(operation.dialect(), operation.operationId(), getInherentAttributes(operation.dialect(), operation.operationId(), operation.attributes(), dialectRegistry), derivedResultAttributes, derivedChildAttributes, childProperties, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> P getProperty(
            String dialect,
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<P> childProperties,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        XOCompanionDialect<C, R, P> companionDialect = physicalModel.companionDialect(dialectRegistry.dialect(dialect));
        return companionDialect.getProperty(operationId, inherentOperationAttributes, derivedResultAttributes, derivedChildAttributes, childProperties);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> List<List<R>> decomposeRequirement(
            Operation operation,
            R requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return decomposeRequirement(operation.dialect(), operation.id(), getInherentAttributes(operation, dialectRegistry), requirement, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> List<List<R>> decomposeRequirement(
            MemoOperation operation,
            R requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return decomposeRequirement(operation.dialect(), operation.operationId(), getInherentAttributes(operation.dialect(), operation.operationId(), operation.attributes(), dialectRegistry), requirement, dialectRegistry, physicalModel);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> List<List<R>> decomposeRequirement(
            String dialect,
            OperationId operationId,
            Attributes inherentOperationAttributes,
            R requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        XOCompanionDialect<C, R, P> companionDialect = physicalModel.companionDialect(dialectRegistry.dialect(dialect));
        return companionDialect.decomposeRequirement(operationId, inherentOperationAttributes, requirement);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> EnforcerPipeline<C, R, P> getEnforcers(
            String dialect,
            Type baseType,
            Attributes derivedBaseAttributes,
            P baseProperty,
            R requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        XOCompanionDialect<C, R, P> companionDialect = physicalModel.companionDialect(dialectRegistry.dialect(dialect));
        return companionDialect.getEnforcers(baseType, derivedBaseAttributes, baseProperty, requirement, dialectRegistry, physicalModel);
    }
}
