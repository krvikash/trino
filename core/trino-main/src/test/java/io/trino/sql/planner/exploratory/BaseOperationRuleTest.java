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

import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;

/**
 * Base class for exploratory rule tests, analogous to {@code BaseRuleTest}.
 */
public abstract class BaseOperationRuleTest
{
    private final OperationRuleTester tester = new OperationRuleTester(TESTING_DIALECT_REGISTRY);

    protected OperationRuleTester tester()
    {
        return tester;
    }
}
