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

import io.trino.sql.newir.Dialect;

/**
 * Family of physical concepts used by one XO optimization run.
 * <p>
 * A physical model binds together the domains for costs, requirements, and
 * properties. It also provides dialect-specific companions that interpret
 * operations in terms of those same domains.
 */
public interface PhysicalModel<C extends Cost, R extends Requirement, P extends Property>
{
    CostDomain<C> costs();

    RequirementDomain<R> requirements();

    PropertyDomain<R, P> properties();

    XOCompanionDialect<C, R, P> companionDialect(Dialect dialect);
}
