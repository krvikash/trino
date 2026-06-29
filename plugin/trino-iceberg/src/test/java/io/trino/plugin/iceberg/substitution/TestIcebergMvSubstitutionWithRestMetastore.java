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
package io.trino.plugin.iceberg.substitution;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.materialization.metastore.client.MaterializationMetastoreClientConfig;
import io.starburst.materialization.metastore.client.RequestAuthenticator;
import io.starburst.materialization.metastore.server.TestingMaterializationMetastoreServer;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs the full MV substitution contract suite with the materialization metastore type set to REST,
 * so every CREATE/REFRESH/REMOVE/RENAME on a substitution MV is written through the HTTP client to
 * the DB-backed metastore with real Iceberg-produced materialization definitions. Substitution reads
 * still come from the coordinator's in-memory index (single cluster), so this primarily validates
 * the REST write-through path end-to-end under the full DDL contract.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionWithRestMetastore
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        PostgreSQLContainer metastoreDb = closeAfterClass(new PostgreSQLContainer("postgres:16"));
        metastoreDb.start();
        TestingMaterializationMetastoreServer metastoreServer = closeAfterClass(new TestingMaterializationMetastoreServer(
                metastoreDb.getJdbcUrl(),
                metastoreDb.getUsername(),
                metastoreDb.getPassword()));

        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(ICEBERG_CATALOG)
                                .setSchema("tpch")
                                .build())
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        binder.bind(RequestAuthenticator.class).toInstance(_ -> {});
                        configBinder(binder).bindConfigDefaults(MaterializationMetastoreClientConfig.class, config -> config.setMetastoreId("id"));
                    }
                })
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addCoordinatorProperty("materialization.metastore.type", "REST")
                .addCoordinatorProperty("materialization.metastore.base-uri", metastoreServer.baseUri().toString())
                .build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.execute("CREATE SCHEMA iceberg.tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }
}
