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
package io.trino.plugin.bigquery.substitution;

import io.trino.Session;
import io.trino.plugin.bigquery.BigQueryQueryRunner;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * MV substitution with BigQuery as the source connector: base tables live in BigQuery, the
 * materialized-view storage lives in Iceberg. BigQuery cannot host substitution MVs itself
 * (SUPPORTS_CREATE_MATERIALIZED_VIEW is false), so this mirrors the JDBC-source variant
 * TestIcebergMvOnJdbcSourceSubstitution rather than the Iceberg-native tests.
 * <p>
 * Requires live GCP credentials via the {@code testing.bigquery.credentials-key} system property.
 */
@Execution(SAME_THREAD)
public class TestBigQueryMvSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // BigQueryQueryRunner already provisions the bigquery catalog, its tpch schema, and a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("bigquery", "tpch");
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

    @Test
    @Disabled("BigQuery connector does not support ADD COLUMN")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}
}
