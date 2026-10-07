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
package io.starburst.materialization.ir;

import io.trino.spi.function.CatalogSchemaFunctionName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSupportedAggregateFunctions
{
    @Test
    void testUnsupportedFunctionHasNoVersion()
    {
        CatalogSchemaFunctionName approxDistinct = new CatalogSchemaFunctionName("system", "builtin", "approx_distinct");

        assertThat(SupportedAggregateFunctions.contains(approxDistinct)).isFalse();
        assertThatThrownBy(() -> SupportedAggregateFunctions.semanticVersion(approxDistinct))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregate function system.builtin.approx_distinct is not supported");
    }
}
