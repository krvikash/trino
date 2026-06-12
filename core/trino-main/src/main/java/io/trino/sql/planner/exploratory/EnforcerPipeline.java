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
import io.trino.sql.planner.exploratory.EnforcerPhysicalHelper.PhysicalResult;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.exploratory.AttributeUtils.getDerivedAttributes;
import static java.util.Objects.requireNonNull;

/**
 * Pipeline of enforcers applied on top of an operation to enforce
 * a required property.
 * <p>
 * The pipeline is modeled as a list of {@link Enforcer}s. Each step is
 * expected to preserve the input type and the input derived attributes.
 * Enforcers are allowed to change only the physical property represented by
 * {@code providedProperty}.
 * <p>
 * The pipeline cost is the cost of the final enforcer subgraph, computed
 * as if the base operation contributed zero cost; that is, the pipeline
 * cost does not include the base operation's own cost. Callers must
 * combine the base operation cost with the pipeline cost to obtain the
 * total cost of operation-plus-enforcers.
 */
public class EnforcerPipeline<C extends Cost, R extends Requirement, P extends Property>
{
    private final List<Enforcer> steps;
    private final P providedProperty;
    private final C cost;

    public EnforcerPipeline(
            Type baseType,
            Attributes derivedBaseAttributes,
            P baseProperty,
            List<Enforcer> steps,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        requireNonNull(baseType, "baseType is null");
        requireNonNull(derivedBaseAttributes, "derivedBaseAttributes is null");
        requireNonNull(baseProperty, "baseProperty is null");
        this.steps = ImmutableList.copyOf(requireNonNull(steps, "steps is null"));
        requireNonNull(dialectRegistry, "dialectRegistry is null");
        requireNonNull(physicalModel, "physicalModel is null");

        // each enforcer must preserve the pipeline type and derived attributes
        // TODO allow strengthening derivedBaseAttributes
        for (Enforcer step : this.steps) {
            checkArgument(step.type().equals(baseType), "all enforcer steps must preserve the pipeline baseType");
            checkArgument(
                    getDerivedAttributes(step.operation(), dialectRegistry).equals(derivedBaseAttributes),
                    "all enforcer steps must preserve the pipeline derivedBaseAttributes");
        }

        EnforcerPhysicalHelper<C, R, P> physicalHelper = new EnforcerPhysicalHelper<>(dialectRegistry, physicalModel);
        // seed the helper with zero base cost so the resulting pipeline cost excludes the base operation's own cost
        PhysicalResult<C, P> currentPhysicalResult = new PhysicalResult<>(
                derivedBaseAttributes,
                physicalModel.costs().zero(),
                physicalModel.properties().canonicalize(baseProperty));
        for (Enforcer step : steps) {
            currentPhysicalResult = physicalHelper.getPhysicalResultForEnforcer(step, currentPhysicalResult);
        }
        this.providedProperty = currentPhysicalResult.property();
        this.cost = currentPhysicalResult.cost();
    }

    public static <C extends Cost, R extends Requirement, P extends Property> EnforcerPipeline<C, R, P> empty(
            Type baseType,
            Attributes derivedBaseAttributes,
            P baseProperty,
            DialectRegistry dialectRegistry,
            PhysicalModel<C, R, P> physicalModel)
    {
        return new EnforcerPipeline<>(baseType, derivedBaseAttributes, baseProperty, ImmutableList.of(), dialectRegistry, physicalModel);
    }

    public List<Enforcer> steps()
    {
        return steps;
    }

    public P providedProperty()
    {
        return providedProperty;
    }

    public C cost()
    {
        return cost;
    }
}
