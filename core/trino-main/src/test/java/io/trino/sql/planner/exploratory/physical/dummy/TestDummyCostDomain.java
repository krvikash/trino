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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.exploratory.physical.CostDomainAssertions.assertDecisionOrderLaws;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.UNKNOWN;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;

class TestDummyCostDomain
{
    @Test
    void testDecisionOrderLaws()
    {
        assertDecisionOrderLaws(DummyCostDomain.INSTANCE, List.of(ZERO, UNKNOWN, INFINITE));
    }
}
