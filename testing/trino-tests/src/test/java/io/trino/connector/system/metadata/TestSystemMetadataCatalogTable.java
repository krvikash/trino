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
package io.trino.connector.system.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.connector.TestDynamicCatalogs;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingCatalogFailureHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestSystemMetadataCatalogTable
        extends AbstractTestQueryFramework
{
    private static final CatalogName BROKEN_CATALOG = new CatalogName("broken_catalog");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        ImmutableMap<String, String> properties = ImmutableMap.of("non_existing", "false");
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setAdditionalModule(combine(
                        new TestDynamicCatalogs.TestCatalogStoreModule(ImmutableMap.of(BROKEN_CATALOG, new CatalogProperties(
                                BROKEN_CATALOG,
                                new CatalogVersion("abc123"),
                                new ConnectorName("memory"),
                                properties))),
                        TestingCatalogFailureHandler.module()))
                .setCoordinatorProperties(ImmutableMap.of("catalog.store", "prepopulated_memory"))
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("healthy_catalog", "memory", ImmutableMap.of("memory.max-data-per-node", "128MB"));
        return queryRunner;
    }

    @Test
    public void testNewCatalogStatus()
    {
        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory', 'OPERATIONAL'), " +
                "('broken_catalog', 'broken_catalog', 'memory', 'FAILING'), " +
                "('system', 'system', 'system', 'OPERATIONAL')");

        TestingCatalogFailureHandler catalogFailureHandler = getQueryRunner().getCoordinator().getInstance(Key.get(TestingCatalogFailureHandler.class));
        assertThat(catalogFailureHandler.getFailures()).satisfies(failures -> {
            assertThat(failures).hasSize(1);
            TestingCatalogFailureHandler.CatalogFailure failure = failures.getFirst();
            assertThat(failure.catalog().getCatalogName()).isEqualTo(BROKEN_CATALOG);
        });
        catalogFailureHandler.reset();

        assertUpdate("CREATE CATALOG brain USING memory WITH (\"memory.max-data-per-node\" = '128MB')");
        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory', 'OPERATIONAL'), " +
                "('broken_catalog', 'broken_catalog', 'memory', 'FAILING'), " +
                "('brain', 'brain', 'memory', 'OPERATIONAL'), " +
                "('system', 'system', 'system', 'OPERATIONAL')");

        assertThat(catalogFailureHandler.getFailures()).isEmpty();

        assertUpdate("DROP CATALOG brain");
    }

    @Test
    public void testCatalogNotLoadedCorrectly()
    {
        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory', 'OPERATIONAL'), " +
                "('broken_catalog', 'broken_catalog', 'memory', 'FAILING'), " +
                "('system', 'system', 'system', 'OPERATIONAL')");
    }
}
