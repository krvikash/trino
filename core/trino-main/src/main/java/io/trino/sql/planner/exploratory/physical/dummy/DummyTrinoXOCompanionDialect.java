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
package io.trino.sql.planner.exploratory.physical.dummy;

import com.google.common.collect.ImmutableList;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.EnforcerPipeline;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ONE;

/**
 * Companion dialect of the dummy physical model. Every operation costs one
 * operation on top of its children, so an operation's cost is strictly worse
 * than the cost of each of its children, and every cycle adds cost.
 */
public final class DummyTrinoXOCompanionDialect
        implements XOCompanionDialect<DummyCost, DummyRequirement, DummyProperty>
{
    public static final DummyTrinoXOCompanionDialect INSTANCE = new DummyTrinoXOCompanionDialect();

    private DummyTrinoXOCompanionDialect() {}

    @Override
    public DummyCost getCost(
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<DummyProperty> childProperties,
            List<DummyCost> childCosts,
            Optional<DummyCost> previousCost)
    {
        return DummyCostDomain.INSTANCE.add(ONE, DummyCostDomain.INSTANCE.addAll(childCosts));
    }

    @Override
    public DummyProperty getProperty(
            OperationId operationId,
            Attributes inherentOperationAttributes,
            Attributes derivedResultAttributes,
            List<Attributes> derivedChildAttributes,
            List<DummyProperty> childProperties)
    {
        return DummyProperty.UNKNOWN;
    }

    @Override
    public List<List<DummyRequirement>> decomposeRequirement(
            OperationId operationId,
            Attributes inherentOperationAttributes,
            DummyRequirement requirement)
    {
        ImmutableList.Builder<DummyRequirement> childRequirements = ImmutableList.builder();
        operationId.argumentTypes().forEach(_ -> childRequirements.add(DummyRequirement.UNDEFINED));
        operationId.regionTypes().forEach(_ -> childRequirements.add(DummyRequirement.UNDEFINED));
        return ImmutableList.of(childRequirements.build());
    }

    @Override
    public EnforcerPipeline<DummyCost, DummyRequirement, DummyProperty> getEnforcers(
            Type baseType,
            Attributes derivedBaseAttributes,
            DummyProperty baseProperty,
            DummyRequirement requirement,
            DialectRegistry dialectRegistry,
            PhysicalModel<DummyCost, DummyRequirement, DummyProperty> physicalModel)
    {
        return EnforcerPipeline.empty(baseType, derivedBaseAttributes, baseProperty, dialectRegistry, physicalModel);
    }
}
