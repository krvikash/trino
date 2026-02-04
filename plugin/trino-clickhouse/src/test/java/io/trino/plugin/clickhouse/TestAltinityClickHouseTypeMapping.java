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
package io.trino.plugin.clickhouse;

import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.ALTINITY_DEFAULT_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAltinityClickHouseTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(ALTINITY_DEFAULT_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer).build();
    }

    @Test
    @Override
    public void testArrayWithTupleElement()
    {
        // Insert syntax and Select return on Array[Tuple] is different in older versions of ClickHouse
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_array_of_tuple",
                "(c1 Array(Tuple(a Int32, b String))) ENGINE=Log",
                List.of("[(1), (2)], [('hello'), ('world')]"))) {
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT (ARRAY[1, 2]), (ARRAY[to_utf8('hello'), to_utf8('world')])");
        }
    }
}
