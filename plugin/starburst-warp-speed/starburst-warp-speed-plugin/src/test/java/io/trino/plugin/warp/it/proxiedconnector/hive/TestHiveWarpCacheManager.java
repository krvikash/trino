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
package io.trino.plugin.warp.it.proxiedconnector.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.OperatorStats;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveWarpCacheManager
        extends DispatcherStubsIntegrationSmokeIT
{
    private static final String TABLE_1 = "table" + StringUtils.randomAlphanumeric(4);
    private static final String TABLE_2 = "table" + StringUtils.randomAlphanumeric(4);

    public TestHiveWarpCacheManager()
    {
        super(1, "hive_cache");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        "hive.s3.aws-access-key", "this is a fake key",
                        USE_HTTP_SERVER_PORT, Boolean.FALSE.toString(),
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of("cache.enabled", "true"));

        Path cacheStorePath = Files.createTempDirectory("cache_");
        ImmutableMap.Builder<String, String> cacheConfigBuilder = ImmutableMap.builder();
        ((DistributedQueryRunner) queryRunner).getServers()
                .forEach(server -> server.getCacheManagerRegistry(
                        "warp_cache",
                        cacheConfigBuilder
                                .put("node.environment", "warp_speed_cache")
                                .put("warp-speed.config.is-single", Boolean.valueOf(numNodes > 1).toString())
//                                .put("cache.enabled", "true")
                                .put("warp-speed.config.is-cache", "true")
                                .put("warp-speed.metrics.dump.interval", "5s")
                                .put("warp-speed.cluster-uuid", "test")
                                .put("http-rest-port", "8098")
                                .put("warp-speed.use-http-server-port", "false")
                                .put("warp-speed.config.http-rest-port-enabled", "true")
                                .put("warp-speed.config.extensions.enabled", "true")
                                .put("warp-speed.call-home.enable", "false")
                                .put("warp-speed.store.path", "file://" + cacheStorePath.toString())
                                .put("warp-speed.local-store.path", cacheStorePath.toString())
                                .buildOrThrow()));
        return queryRunner;
    }

    @Test
    public void testSimpleWarm()
    {
        prepare();
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        @Language("SQL") String query = "select int1, v1 from " + TABLE_1 + " where v1 like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query);

        @Language("SQL") String query2 = "select int1, v1 from " + TABLE_2 + " where v1 like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query2);

        DistributedQueryRunner queryRunner2 = getDistributedQueryRunner();
        @Language("SQL") String unionQuery = "select * from %s b where b.int1 > 0 union all select * from %s".formatted(TABLE_1, TABLE_2);
        runQueryAndValidateReadFromCache(queryRunner2, unionQuery);
        assertExplain("explain " + unionQuery, "CacheData\\[\\]\n.*\n.*TableScan.*");
    }

    /**
     * run the same query twice - first run it should store in cache, second should read from cache
     */
    private void runQueryAndValidateReadFromCache(DistributedQueryRunner queryRunner, @Language("SQL") String query)
    {
        MaterializedResultWithPlan firstRun = queryRunner.executeWithPlan(getSession(), query);
        List<String> firstRunOperatorTypes = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(firstRun.queryId()).getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getOperatorType).collect(Collectors.toList());
        assertThat(firstRunOperatorTypes).doesNotContain("LoadCachedDataOperator");

        MaterializedResultWithPlan secondRun = queryRunner.executeWithPlan(getSession(), query);
        List<String> secondRunOperatorTypes = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(secondRun.queryId()).getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getOperatorType).collect(Collectors.toList());
        assertThat(secondRunOperatorTypes).contains("LoadCachedDataOperator");
    }

    /**
     * warm table_1, table_2 with default warming.
     * int_1- DATA, BASIC. v1- DATA, BASIC, LUCENE
     */
    private void prepare()
    {
        createTable(DEFAULT_SCHEMA,
                TABLE_1,
                "(int1 integer, v1 varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO %s VALUES (1, 'shlomi'), (2, 'kobi')".formatted(TABLE_1));
        createTable(DEFAULT_SCHEMA,
                TABLE_2,
                "(int1 integer, v1 varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO %s VALUES (3, 'roman'), (4, 'tal')".formatted(TABLE_2));
        String query2 = "select int1, v1 from " + TABLE_2 + " where int1 > 0 and v1 like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query2, true, 5, 2);
        //warm int_1 (DATA, BASIC), v1 (DATA, BASIC, LUCENE) with default warming
        String query = "select int1, v1 from " + TABLE_1 + " where int1 > 0 and v1 like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query, true, 5, 2);
    }

    @Override
    @Test
    public void testGoAllProxyOnlyWhenHavePushDowns()
    {
        //not relevant
    }
}
