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
package io.trino.plugin.singlestore.substitution;

import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.singlestore.SingleStoreQueryRunner;
import io.trino.plugin.singlestore.TestingSingleStoreServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Requires the SingleStore test container, which needs a SingleStore license; runs in CI's
 * SingleStore job.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnSingleStoreSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        TestingSingleStoreServer singleStoreServer = closeAfterClass(new TestingSingleStoreServer());
        return SingleStoreQueryRunner.builder(singleStoreServer)
                .addCoordinatorProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // SingleStoreQueryRunner already provisions the source catalog, its tpch schema, and a tpch catalog.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("singlestore", "tpch");
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
