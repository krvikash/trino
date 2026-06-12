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
import io.trino.sql.newir.Block;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.planner.exploratory.AttributeUtils.getDerivedAttributes;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.getCost;
import static io.trino.sql.planner.exploratory.XOCompanionDialectUtils.getProperty;
import static java.util.Objects.requireNonNull;

/**
 * A helper class to compute physical properties for {@link EnforcerPipeline}.
 */
final class EnforcerPhysicalHelper<C extends Cost, R extends Requirement, P extends Property>
{
    private final DialectRegistry dialectRegistry;
    private final PhysicalModel<C, R, P> physicalModel;

    EnforcerPhysicalHelper(DialectRegistry dialectRegistry, PhysicalModel<C, R, P> physicalModel)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
        this.physicalModel = requireNonNull(physicalModel, "physicalModel is null");
    }

    PhysicalResult<C, P> getPhysicalResultForEnforcer(Enforcer enforcer, PhysicalResult<C, P> basePhysicalResult)
    {
        // per Enforcer contract, the base operation result is the only argument
        Operation enforcerOperation = enforcer.operation();
        checkState(enforcerOperation.arguments().size() == 1 && enforcerOperation.arguments().getFirst() instanceof Operation.Result, "enforcer operation must have exactly one argument");

        // the physical result for the base operation has been passed
        // the physical results for the regions must be computed
        Map<Operation.Result, PhysicalResult<C, P>> physicalResults = new HashMap<>();
        physicalResults.put((Operation.Result) enforcerOperation.arguments().getFirst(), basePhysicalResult);
        return getPhysicalResultForOperation(enforcerOperation, physicalResults);
    }

    private PhysicalResult<C, P> getPhysicalResultForOperation(Operation operation, Map<Operation.Result, PhysicalResult<C, P>> physicalResults)
    {
        List<Attributes> childAttributes = new ArrayList<>();
        List<C> childCosts = new ArrayList<>();
        List<P> childProperties = new ArrayList<>();
        for (Value argument : operation.arguments()) {
            switch (argument) {
                case Operation.Result result -> {
                    PhysicalResult<C, P> physicalResult = physicalResults.get(result);
                    checkState(physicalResult != null, "physical result for %s is not available", result);
                    childAttributes.add(physicalResult.derivedAttributes);
                    childCosts.add(physicalResult.cost);
                    childProperties.add(physicalResult.property);
                }
                case Block.Parameter _ -> {
                    childAttributes.add(DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
                    childCosts.add(physicalModel.costs().parameter());
                    childProperties.add(physicalModel.properties().scalar());
                }
            }
        }
        for (Region region : operation.regions()) {
            Block block = region.getOnlyBlock();
            PhysicalResult<C, P> physicalResult = getPhysicalResultForBlock(block, new HashMap<>(physicalResults));
            childAttributes.add(physicalResult.derivedAttributes);
            childCosts.add(physicalResult.cost);
            childProperties.add(physicalResult.property);
        }

        Attributes derivedAttributes = getDerivedAttributes(operation, dialectRegistry);
        C cost = getCost(operation, derivedAttributes, childAttributes, childProperties, childCosts, Optional.empty(), dialectRegistry, physicalModel);
        P property = physicalModel.properties().canonicalize(getProperty(operation, derivedAttributes, childAttributes, childProperties, dialectRegistry, physicalModel));
        return new PhysicalResult<>(derivedAttributes, cost, property);
    }

    // Does not account for reuse. Cost of an operation is considered separately for each usage.
    private PhysicalResult<C, P> getPhysicalResultForBlock(Block block, Map<Operation.Result, PhysicalResult<C, P>> physicalResults)
    {
        for (Operation operation : block.operations()) {
            PhysicalResult<C, P> operationPhysicalResult = getPhysicalResultForOperation(operation, physicalResults);
            physicalResults.put(operation.result(), operationPhysicalResult);
        }
        return physicalResults.get(block.getTerminalOperation().result());
    }

    record PhysicalResult<C extends Cost, P extends Property>(Attributes derivedAttributes, C cost, P property)
    {
        PhysicalResult
        {
            requireNonNull(derivedAttributes, "derivedAttributes is null");
            requireNonNull(cost, "cost is null");
            requireNonNull(property, "property is null");
        }
    }
}
