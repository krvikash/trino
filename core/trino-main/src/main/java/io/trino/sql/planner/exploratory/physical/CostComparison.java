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

/**
 * Result of comparing two costs in a partial order.
 */
public enum CostComparison
{
    /**
     * The left cost is preferable to the right cost.
     */
    BETTER,

    /**
     * The costs are equal under the domain's ordering.
     */
    EQUAL,

    /**
     * The left cost is less preferable than the right cost.
     */
    WORSE,

    /**
     * Neither cost is provably preferable to the other.
     */
    INCOMPARABLE,
}
