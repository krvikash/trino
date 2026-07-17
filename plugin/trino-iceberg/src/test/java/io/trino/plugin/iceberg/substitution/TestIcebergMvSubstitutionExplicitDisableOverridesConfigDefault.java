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
import io.starburst.server.substitution.MaterializedViewSubstitutionConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Verifies that an explicit {@code materialized-view-substitution.support.enabled=false} wins over a module that
 * raises the default to {@code true} via {@code bindConfigDefaults}, leaving substitution disabled. Airlift treats
 * {@code bindConfigDefaults} as a default that an explicit property overrides, so an operator (or portal) can
 * always turn substitution off even when an embedding assembly (e.g. SEP) would enable it by default. When
 * disabled, the substitution wiring is absent — observable here because the {@code substitution_enabled}
 * materialized view property is not registered.
 * <p>
 * NOTE: this pairs with {@link TestIcebergMvSubstitutionEnabledByConfigDefaults}; once substitution is enabled by
 * default, both tests must be revisited.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionExplicitDisableOverridesConfigDefault
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(ICEBERG_CATALOG)
                                .setSchema("tpch")
                                .build())
                // Explicitly disabled by the operator; the module below tries to default it on, but the explicit
                // property must win, keeping the feature off.
                .addExtraProperty("materialized-view-substitution.support.enabled", "false")
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        configBinder(binder).bindConfigDefaults(
                                MaterializedViewSubstitutionConfig.class,
                                config -> config.setMaterializedViewSubstitutionSupportEnabled(true));
                    }
                })
                .build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.execute("CREATE SCHEMA iceberg.tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Test
    public void testSubstitutionDisabledDespiteConfigDefault()
    {
        assertUpdate("CREATE TABLE base_table AS SELECT 1 AS id", 1);
        // The substitution_enabled MV property is only registered when the feature is enabled, so its absence
        // proves the explicit property disabled the feature despite the raised default.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_mv WITH (substitution_enabled = true) AS SELECT * FROM base_table",
                ".*materialized view property 'substitution_enabled' does not exist.*");
    }
}
