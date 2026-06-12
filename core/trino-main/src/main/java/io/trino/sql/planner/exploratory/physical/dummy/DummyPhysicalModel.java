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

import io.trino.sql.newir.Dialect;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;

import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.lang.String.format;

public final class DummyPhysicalModel
        implements PhysicalModel<DummyCost, DummyRequirement, DummyProperty>
{
    public static final DummyPhysicalModel INSTANCE = new DummyPhysicalModel();

    private final XOCompanionDialect<DummyCost, DummyRequirement, DummyProperty> trinoCompanionDialect = DummyTrinoXOCompanionDialect.INSTANCE;

    private DummyPhysicalModel() {}

    @Override
    public CostDomain<DummyCost> costs()
    {
        return DummyCostDomain.INSTANCE;
    }

    @Override
    public RequirementDomain<DummyRequirement> requirements()
    {
        return DummyRequirementDomain.INSTANCE;
    }

    @Override
    public PropertyDomain<DummyRequirement, DummyProperty> properties()
    {
        return DummyPropertyDomain.INSTANCE;
    }

    @Override
    public XOCompanionDialect<DummyCost, DummyRequirement, DummyProperty> companionDialect(Dialect dialect)
    {
        if (!TRINO.equals(dialect.name())) {
            throw new IllegalArgumentException(format("No XO companion dialect registered for dialect %s in model %s", dialect.name(), getClass().getSimpleName()));
        }
        return trinoCompanionDialect;
    }
}
