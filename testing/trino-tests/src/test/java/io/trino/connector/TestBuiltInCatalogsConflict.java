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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.testing.TempFile;
import io.trino.cache.CacheManagerRegistry;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBuiltInCatalogsConflict
{
    @TempDir
    public static Path catalogConfigDir;

    @Test
    void testConflictingCatalogDefinition()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createQueryRunner();
        // it's hard to instantiate a catalog factory, so grab it from the query runner
        CatalogFactory catalogFactory =
                queryRunner.getCoordinator().getInstance(Key.get(CatalogFactory.class));
        CacheManagerRegistry cacheManagerRegistry =
                queryRunner.getCoordinator().getInstance(Key.get(CacheManagerRegistry.class));

        Files.writeString(
                catalogConfigDir.resolve("foo.properties"),
                "connector.name=tpch",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        try (TempFile tempFile = new TempFile()) {
            Files.writeString(tempFile.path(), "catalog.config-dir=" + catalogConfigDir);
            CatalogStoreConfig catalogStoreConfig = new CatalogStoreConfig().setCatalogStoreKind("file");
            CatalogStoreManager catalogStoreManager = new CatalogStoreManager(new SecretsResolver(ImmutableMap.of()), catalogStoreConfig);
            catalogStoreManager.loadConfiguredCatalogStore(catalogStoreConfig.getCatalogStoreKind(), tempFile.file());
            CoordinatorDynamicCatalogManager catalogManager = new CoordinatorDynamicCatalogManager(
                    catalogStoreManager,
                    catalogFactory,
                    () ->
                            ImmutableList.of(
                                    new CatalogStore.StoredCatalog()
                                    {
                                        @Override
                                        public CatalogName name()
                                        {
                                            return new CatalogName("foo");
                                        }

                                        @Override
                                        public CatalogProperties loadProperties()
                                        {
                                            return new CatalogProperties(
                                                    name(),
                                                    new CatalogVersion("version"),
                                                    new ConnectorName("tpch"),
                                                    ImmutableMap.of());
                                        }
                                    }),
                    ImmutableSet.of(),
                    cacheManagerRegistry,
                    Executors.newSingleThreadScheduledExecutor());
            assertThatThrownBy(catalogManager::loadInitialCatalogs)
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("Catalog name foo is reserved by Starburst");
        }
    }

    private DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build()).build();
    }
}
