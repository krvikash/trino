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
package io.trino.tests.benchmark;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestTpcds
{
    @Test
    void testNormalizeQuery()
    {
        assertThat(Tpcds.normalizeQuery("1")).isEqualTo("q01");
        assertThat(Tpcds.normalizeQuery("01")).isEqualTo("q01");
        assertThat(Tpcds.normalizeQuery("q01")).isEqualTo("q01");

        for (String query : Tpcds.allQueries()) {
            assertThat(Tpcds.normalizeQuery(query)).isEqualTo(query);
        }
    }
}
