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

import io.trino.sql.planner.exploratory.physical.PropertyDomain;

import static io.trino.sql.planner.exploratory.physical.dummy.DummyProperty.SCALAR;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyProperty.UNKNOWN;

public final class DummyPropertyDomain
        implements PropertyDomain<DummyRequirement, DummyProperty>
{
    public static final DummyPropertyDomain INSTANCE = new DummyPropertyDomain();

    private DummyPropertyDomain() {}

    @Override
    public DummyProperty scalar()
    {
        return SCALAR;
    }

    @Override
    public DummyProperty canonicalize(DummyProperty property)
    {
        return property;
    }

    @Override
    public boolean satisfies(DummyProperty property, DummyRequirement requirement)
    {
        return true;
    }

    @Override
    public DummyProperty minimalSatisfying(DummyRequirement requirement)
    {
        return UNKNOWN;
    }
}
