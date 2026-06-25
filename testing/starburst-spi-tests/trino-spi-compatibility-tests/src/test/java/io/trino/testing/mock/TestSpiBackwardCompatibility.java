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
package io.trino.testing.mock;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

@Disabled("TODO: CompiledWithOssSplitManager needs to implement getSplits(Set<ColumnHandle>, Constraint) for new split SPI")
final class TestSpiBackwardCompatibility
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // TODO: TestingTrinoServer does not perform plugin classloader isolation
        //   consider adding that to make the test closer to production setup
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSession()).build();
        queryRunner.installPlugin(new CompiledWithOssPlugin());
        queryRunner.createCatalog("mock", "mock", Map.of());
        return queryRunner;
    }

    @Test
    void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM mock");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("default");
    }

    @Test
    void testShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM mock.\"default\"");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("test_table");
    }

    @Test
    void testSelectFromTable()
    {
        MaterializedResult result = computeActual("SELECT col1, col2 FROM mock.\"default\".test_table");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .containsExactly("hello", "world");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(1)))
                .containsExactly("foo", "bar");
    }

    @Test
    void testProjection()
    {
        MaterializedResult result = computeActual("SELECT col1 || col2 FROM mock.\"default\".test_table");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .containsExactly("hellofoo", "worldbar");
    }

    @Test
    void testCustomTypeProjection()
    {
        assertThat(computeActual("SELECT COUNT(custom_from_varchar(col1)) FROM mock.\"default\".test_table").getOnlyValue())
                .isEqualTo(2L);
    }

    @Test
    void testDescribeTable()
    {
        MaterializedResult result = computeActual("DESCRIBE mock.\"default\".test_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("col1", "col2", "col3");
    }
}
