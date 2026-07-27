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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs the full MV substitution contract suite with {@code materialized-view-substitution.support.enabled}
 * left unset (its default is disabled), enabling the feature instead through a module that raises the config
 * default via {@code bindConfigDefaults}. This mirrors how an embedding assembly (e.g. SEP) turns substitution
 * on from its own wiring rather than an operator property, and verifies that {@link
 * io.starburst.server.substitution.MvSubstitutionModule} honors that default when deciding to wire the real
 * {@code MaterializationService} and {@code MaterializationIndex}.
 * <p>
 * NOTE: this relies on the config default being disabled. Once substitution is enabled by default (the field
 * default of {@code materialized-view-substitution.support.enabled} flips to {@code true}), leaving the property
 * unset would already enable the feature and this test would no longer exercise the {@code bindConfigDefaults}
 * enable path — it must be revisited then.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionEnabledByConfigDefaults
        extends AbstractIcebergOnIcebergMvSubstitutionTest
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
                // Note: the property is intentionally not set here; the feature is enabled solely through the
                // config default raised in the additional module below.
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
