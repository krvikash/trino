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
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.tpch.TpchConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class TestBuiltInCatalogs
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner
                .builder(TestingSession.testSessionBuilder().build())
                .setAdditionalSetup(runner -> runner.installPlugin(new TpchPlugin()))
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        configBinder(binder).bindConfig(TpchConfig.class);
                        newOptionalBinder(binder, BuiltInCatalogsProvider.class).setBinding().toInstance(
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
                                                }));
                    }
                })
                .setWorkerCount(2)
                .build();
    }

    @Test
    void testBuiltInCatalogs()
    {
        assertQuery("SHOW CATALOGS", "VALUES 'foo', 'system'");
    }

    @Test
    void testCreateBuiltInCatalogs()
    {
        assertQueryFails("CREATE CATALOG foo USING tpch WITH(\"tpch.max-rows-per-page\"='100')", "Catalog 'foo' already exists");
        assertQueryFails("CREATE CATALOG Foo USING tpch WITH(\"tpch.max-rows-per-page\"='100')", "Catalog 'foo' already exists");
        assertQueryFails("CREATE CATALOG system USING tpch WITH(\"tpch.max-rows-per-page\"='100')", "Catalog 'system' already exists");
    }

    @Test
    void testAlterBuiltInCatalogs()
    {
        assertQueryFails("ALTER CATALOG foo SET PROPERTIES \"tpch.max-rows-per-page\"='100'", "Altering built-in catalog foo is not allowed");
        assertQueryFails("ALTER CATALOG Foo SET PROPERTIES \"tpch.max-rows-per-page\"='100'", "Altering built-in catalog foo is not allowed");
        // cannot alter catalog system, as the connector has no properties
    }

    @Test
    void testDropBuiltInCatalogs()
    {
        assertQueryFails("DROP CATALOG foo", "Dropping built-in catalog foo is not allowed");
        assertQueryFails("DROP CATALOG Foo", "Dropping built-in catalog foo is not allowed");
        assertQueryFails("DROP CATALOG System", "Dropping system catalog is not allowed");
    }

    @Test
    void testRenameBuiltInCatalogs()
    {
        assertQueryFails("ALTER CATALOG foo RENAME TO bar", "Renaming built-in catalog foo is not allowed");
        // this fails with io.trino.testing.QueryFailedException: No value present
        // assertQueryFails("ALTER CATALOG system RENAME TO bar", "Renaming built-in catalog foo is not allowed");
    }

    @Test
    void testQueryBuiltInCatalogs()
    {
        assertQuery("SELECT count(*) FROM foo.sf1.nation", "VALUES 25");
    }
}
