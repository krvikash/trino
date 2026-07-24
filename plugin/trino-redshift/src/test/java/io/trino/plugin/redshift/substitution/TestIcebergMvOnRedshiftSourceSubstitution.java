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
package io.trino.plugin.redshift.substitution;

import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.redshift.RedshiftQueryRunner;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Requires real Redshift + AWS credentials ({@code test.redshift.*}); run in CI via the
 * {@code cloud-tests} profile, not in the default build.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnRedshiftSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .addCoordinatorProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // RedshiftQueryRunner already provisions the source catalog, its test schema, and a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("redshift", TEST_SCHEMA);
    }

    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }
}
