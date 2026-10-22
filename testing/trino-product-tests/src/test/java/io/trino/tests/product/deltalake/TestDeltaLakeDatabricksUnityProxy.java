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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(DatabricksUnityProxyEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricksUnityProxy
@TestGroup.ProfileSpecificTests
class TestDeltaLakeDatabricksUnityProxy
{
    private final String schemaName = "test_delta_basic_" + randomNameSuffix();
    private String unityCatalogName;

    @BeforeEach
    void setUp(DatabricksUnityProxyEnvironment env)
    {
        unityCatalogName = env.unityCatalogName();
        env.executeDatabricksSql(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterEach
    void cleanUp(DatabricksUnityProxyEnvironment env)
    {
        env.executeDatabricksSql(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testShowSchemas(DatabricksUnityProxyEnvironment env)
    {
        assertThat(env.executeTrinoSql(format("SHOW SCHEMAS FROM delta LIKE '%s'", schemaName)))
                .contains(row(schemaName));
    }
}
