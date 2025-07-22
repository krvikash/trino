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
package io.trino.plugin.warp.it.proxiedconnector.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.FileTestingTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.LocalTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestDeltaLakeProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "warp_deltalake");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(new WarpStubsStorageEngineModule(),
                Optional.of(binder -> {
                    MapBinder<String, TransactionLogSynchronizer> logSynchronizerMapBinder = newMapBinder(binder, String.class, TransactionLogSynchronizer.class);
                    logSynchronizerMapBinder.addBinding("file").to(FileTestingTransactionLogSynchronizer.class).in(Scopes.SINGLETON);
                    LocalFileSystemFactory localFileSystemFactory = new LocalFileSystemFactory(hiveDir);
                    newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                            .addBinding("local").toInstance(localFileSystemFactory);
                    newMapBinder(binder, String.class, TransactionLogSynchronizer.class)
                            .addBinding("local").toInstance(new LocalTransactionLogSynchronizer(new DefaultDeltaLakeFileSystemFactory(localFileSystemFactory)));
                    configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, defaults -> defaults.setCatalogDirectory("local:///"));
                }),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, DELTA_LAKE_CONNECTOR_NAME,
                        "delta.enable-non-concurrent-writes", "true"),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Collections.emptyMap());
    }

    @Test
    public void testSimpleWithoutWarmReturnProxy()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(format("SELECT %s FROM t WHERE %s = 1", C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void test_CTAS()
    {
        computeActual("CREATE TABLE t2 AS SELECT * FROM t");
        computeActual("DROP TABLE t2");
    }

    @Test
    public void testCount()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        MaterializedResult result = computeActual(getSession(), "select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1); // collect from hive
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        String jmxTable = "io.trino.plugin.warp.gen.stats:*,name=dispatcherpagesource_" + catalog + "_*,type=dispatcherpagesourcestats";
        computeActual(createJmxSession(), "show tables");
        MaterializedRow statsMaterializedRow = getServiceStats(createJmxSession(),
                jmxTable,
                ImmutableList.of("empty_collect_columns"));
        assertThat((long) statsMaterializedRow.getField(0))
                .describedAs("empty_collect_columns is none zero")
                .isZero();

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t",
                false,
                2,
                1);

        result = computeActual(getSession(), "select count(%s) from t".formatted(C1));
        assertThat(result.getRowCount()).isEqualTo(1); // collect from row group
//        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        statsMaterializedRow = getServiceStats(createJmxSession(),
                jmxTable,
                ImmutableList.of("empty_collect_columns"));
        assertThat((long) statsMaterializedRow.getField(0))
                .describedAs("empty_collect_columns is none zero")
                .isEqualTo(0);
    }
}
