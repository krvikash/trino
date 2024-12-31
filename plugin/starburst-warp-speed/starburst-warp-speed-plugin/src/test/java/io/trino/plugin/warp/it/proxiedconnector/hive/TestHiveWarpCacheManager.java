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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.config.CacheManagerConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.warmup.demoter.TupleRankResult;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterThreshold;
import io.trino.plugin.warp.extension.execution.warmup.CacheMgrWarmupTask;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.plugin.warp.tools.util.CompressionUtil;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import io.trino.spi.cache.PlanSignature;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import jakarta.ws.rs.HttpMethod;
import org.apache.commons.io.FileUtils;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.dispatcher.DispatcherCacheManagerFactory.DISPATCHER_CACHE_MANAGER_NAME;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveWarpCacheManager
        extends DispatcherStubsIntegrationSmokeIT
{
    private static final Logger logger = Logger.get(TestHiveWarpCacheManager.class);

    private static final String TABLE_1 = "table" + StringUtils.randomAlphanumeric(4);
    private static final String TABLE_2 = "table" + StringUtils.randomAlphanumeric(4);

    private static final String scanFilterAndProjectOperatorName = ScanFilterAndProjectOperator.class.getSimpleName();
    private static final String tableScanOperatorName = TableScanOperator.class.getSimpleName();
    private static final String loadCachedDataOperatorName = LoadCachedDataOperator.class.getSimpleName();
    private static final String COL_INT_1 = "int1";
    private static final String COL_V_1 = "v1";

    private int coordinatorPort;
    private final Path cacheMgrPath;
    private final Path cacheMgrFetcherPath;

    public TestHiveWarpCacheManager()
    {
        super(1, "hive_cache");

        try {
            cacheMgrPath = Files.createTempDirectory("cache_mgr");
            cacheMgrFetcherPath = Files.createTempDirectory("cache_mgr_fetcher");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        ((DistributedQueryRunner) queryRunner).getServers()
                .forEach(server -> {
                    WarpExtensionConfig warpExtensionConfig = new WarpExtensionConfig();
                    warpExtensionConfig.setUseHttpServerPort(false);
                    warpExtensionConfig.setRestHttpPort(server.getBaseUrl().getPort() + 3);

                    if (server.isCoordinator()) {
                        coordinatorPort = warpExtensionConfig.getRestHttpPort();
                    }

                    ImmutableMap.Builder<String, String> cacheConfigBuilder = ImmutableMap.builder();
                    server.getCacheManagerRegistry(
                            DISPATCHER_CACHE_MANAGER_NAME,
                            cacheConfigBuilder
                                    .put(GlobalConfig.CONFIG_IS_SINGLE, Boolean.valueOf(numNodes == 1).toString())
                                    .put(GlobalConfig.LOCAL_STORE_PATH, "file://" + cacheMgrPath.toAbsolutePath())
                                    .put(WarmupRuleCloudFetcherConfig.STORE_PATH, "file://%s/rules".formatted(cacheMgrFetcherPath.toAbsolutePath()))
                                    .put(CacheManagerConfig.CACHE_MANAGER_RULES_ENABLED, Boolean.TRUE.toString())
                                    .put(WarpExtensionConfig.ENABLED, Boolean.TRUE.toString())
                                    .put(WarpExtensionConfig.USE_HTTP_SERVER_PORT, Boolean.toString(warpExtensionConfig.isUseHttpServerPort()))
                                    .put(WarpExtensionConfig.HTTP_REST_PORT_ENABLED, Boolean.toString(warpExtensionConfig.isRestHttpDefaultPortEnabled()))
                                    .put(WarpExtensionConfig.HTTP_REST_PORT, Integer.toString(warpExtensionConfig.getRestHttpPort()))
                                    .put("node.environment", "warp")
                                    .buildOrThrow());
                });
        return queryRunner;
    }

    @BeforeEach
    @Override
    public void beforeMethod(TestInfo testInfo)
    {
        super.beforeMethod(testInfo);
        prepare();
    }

    @AfterEach
    @Override
    public void afterMethod(TestInfo testInfo)
    {
        deleteCacheRulesFile(getCacheRulesPath());
        super.afterMethod(testInfo);
    }

    @Override
    @Test
    @Disabled
    public void testGoAllProxyOnlyWhenHavePushDowns() {}

    @Test
    public void testWithoutRules()
            throws IOException
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        prepareCacheMgrRules(List.of());

        @Language("SQL") String query = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_1 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query);

        @Language("SQL") String query2 = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_2 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query2);

        @Language("SQL") String unionQuery = ("select * from %s b where b." + COL_INT_1 + " > 0 union all select * from %s").formatted(TABLE_1, TABLE_2);
        runQueryAndValidateReadFromCache(queryRunner, unionQuery);
        assertExplain("explain " + unionQuery, "CacheData\\[\\]\n.*\n.*TableScan.*");
    }

    @Test
    public void testWithRulesNoMatch()
            throws IOException
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        prepareCacheMgrRules(List.of("key"));

        @Language("SQL") String query = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_1 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query, false);

        @Language("SQL") String query2 = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_2 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query2, false);

        @Language("SQL") String unionQuery = ("select * from %s b where b." + COL_INT_1 + " > 0 union all select * from %s").formatted(TABLE_1, TABLE_2);
        runQueryAndValidateReadFromCache(queryRunner, unionQuery, false);
        assertExplain("explain " + unionQuery, "CacheData\\[\\]\n.*\n.*TableScan.*");
    }

    @Test
    public void testWithRules()
            throws IOException
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        //prepare irrelevant warmup rules so the initial list is not empty
        prepareCacheMgrRules(List.of("dummy"));

        @Language("SQL") String query = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_1 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query, true);

        @Language("SQL") String query2 = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_2 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query2, true);

        @Language("SQL") String unionQuery = ("select * from %s b where b." + COL_INT_1 + " > 0 union all select * from %s").formatted(TABLE_1, TABLE_2);
        runQueryAndValidateReadFromCache(queryRunner, unionQuery, true);
        assertExplain("explain " + unionQuery, "CacheData\\[\\]\n.*\n.*TableScan.*");
    }

    @Test
    public void testDemoteWithRules()
            throws IOException
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        //prepare irrelevant warmup rules so the initial list is not empty
        prepareCacheMgrRules(List.of("dummy"));

        @Language("SQL") String query = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_1 + " where " + COL_V_1 + " like '%shlomi%'";
        runQueryAndValidateReadFromCache(queryRunner, query, true);

        demote(WarmupDemoterData.builder()
                .batchSize(1)
                .modifyConfig(true)
                .warmupDemoterThreshold(new WarmupDemoterThreshold(0.95, 0.1))
                .build());

        validateDemoter(
                new DemoteInput(catalog, 0, 10),
                new DemoteInput(DISPATCHER_CACHE_MANAGER_NAME, 0, 2));
    }

    /**
     * run the same query twice - first run it should store in cache, second should read from cache
     */
    private void runQueryAndValidateReadFromCache(
            DistributedQueryRunner queryRunner,
            @Language("SQL") String query)
    {
        MaterializedResultWithPlan materializedResultWithPlan = queryRunner.executeWithPlan(getSession(), query);
        Set<String> operatorTypes = getFullQueryInfo(queryRunner, materializedResultWithPlan)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .map(OperatorStats::getOperatorType)
                .collect(Collectors.toSet());
        assertThat(operatorTypes)
                .contains(scanFilterAndProjectOperatorName)
                .doesNotContain(loadCachedDataOperatorName);

        materializedResultWithPlan = queryRunner.executeWithPlan(getSession(), query);
        Set<String> operatorTypesTmp = getFullQueryInfo(queryRunner, materializedResultWithPlan)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .map(OperatorStats::getOperatorType)
                .collect(Collectors.toSet());
        assertThat(operatorTypesTmp)
                .contains(loadCachedDataOperatorName)
                .doesNotContain(scanFilterAndProjectOperatorName, tableScanOperatorName);
    }

    /**
     * run the same query twice - first run it should store in cache, second should read from cache
     */
    private void runQueryAndValidateReadFromCache(
            DistributedQueryRunner queryRunner,
            @Language("SQL") String query,
            boolean withSignature)
            throws IOException
    {
        warmAndValidateCacheQuery(queryRunner, query);

        MaterializedResultWithPlan materializedResultWithPlan = queryRunner.executeWithPlan(getSession(), query);

        QueryInfo fullQueryInfo = getFullQueryInfo(queryRunner, materializedResultWithPlan);
        Set<String> operatorTypes = fullQueryInfo.getQueryStats()
                .getOperatorSummaries()
                .stream()
                .map(OperatorStats::getOperatorType)
                .collect(Collectors.toSet());
        assertThat(operatorTypes)
                .contains(loadCachedDataOperatorName)
                .doesNotContain(scanFilterAndProjectOperatorName);

        if (withSignature) {
            List<PlanSignature> planSignatures = requireNonNull(fullQueryInfo
                    .getOutputStage()
                    .orElseThrow()
                    .getPlan())
                    .getRoot()
                    .getSources()
                    .getFirst()
                    .getSources()
                    .stream()
                    .filter(planNode -> planNode instanceof LoadCachedDataPlanNode)
                    .map(planNode -> ((LoadCachedDataPlanNode) planNode).getPlanSignature().signature())
                    .toList();

            prepareCacheMgrRules(planSignatures.stream().map(planSignature -> planSignature.getKey().toString()).toList());
        }

        MaterializedResultWithPlan materializedResultWithPlanAfterCaching = queryRunner.executeWithPlan(getSession(), query);

        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionFailedError.class)
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(2))
                        .build())
                .run(() -> {
                    Set<String> operatorTypesTmp = getFullQueryInfo(queryRunner, materializedResultWithPlanAfterCaching)
                            .getQueryStats()
                            .getOperatorSummaries()
                            .stream()
                            .map(OperatorStats::getOperatorType)
                            .collect(Collectors.toSet());
                    assertThat(operatorTypesTmp).contains(TestHiveWarpCacheManager.loadCachedDataOperatorName);
                });

        //make sure that when rules are used they priority is non-zero
        if (withSignature) {
            String resultStr = executeRestCommand(
                    WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                    WarmupDemoterTask.WARMUP_DEMOTER_TUPLE_RANKS_TASK_NAME,
                    null,
                    HttpMethod.GET,
                    coordinatorPort,
                    HttpURLConnection.HTTP_OK,
                    false);
            Map<String, TupleRankResult> result = objectMapper.readerFor(new TypeReference<Map<String, TupleRankResult>>() {})
                    .readValue(resultStr);
            double priority = result.values()
                    .stream()
                    .map(tupleRankResult -> tupleRankResult.tupleRankList()
                            .stream()
                            .map(tupleRank -> tupleRank.warmupProperties().priority())
                            .distinct()
                            .findFirst()
                            .orElse(0D))
                    .distinct()
                    .findFirst()
                    .orElse(0D);
            assertThat(priority).isPositive();
        }
    }

    private void warmAndValidateCacheQuery(
            DistributedQueryRunner queryRunner,
            @Language("SQL") String query)
    {
        Session jmxSession = createJmxSession();
        String warmStatsTableName = "%s:name=%s_%s,type=%s".formatted(
                WarmingServiceStats.class.getPackageName(),
                WarmingServiceStats.createKey(),
                DISPATCHER_CACHE_MANAGER_NAME,
                WarmingServiceStats.class.getSimpleName().toLowerCase(Locale.ROOT));
        List<String> jmxCounters = List.of("warm_warp_cache_started", "warm_warp_cache_accomplished");
        MaterializedRow materializedRow = getServiceStats(jmxSession, warmStatsTableName, jmxCounters);
        Long valueCacheStartedBefore = (Long) materializedRow.getFields().getFirst();
        Long valueCacheAccomplishedBefore = (Long) materializedRow.getFields().get(1);

        //"warm" data query
        MaterializedResultWithPlan materializedResultWithPlan = queryRunner.executeWithPlan(getSession(), query);

        Set<String> operatorTypes = getFullQueryInfo(queryRunner, materializedResultWithPlan)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .map(OperatorStats::getOperatorType)
                .collect(Collectors.toSet());
        assertThat(operatorTypes)
                .contains(scanFilterAndProjectOperatorName)
                .doesNotContain(loadCachedDataOperatorName);

        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionFailedError.class)
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(1))
                        .build())
                .run(() -> {
                    MaterializedRow materializedRowTmp = getServiceStats(jmxSession, warmStatsTableName, jmxCounters);
                    Long valueCacheStartedAfter = (Long) materializedRowTmp.getFields().getFirst();
                    Long valueCacheAccomplishedAfter = (Long) materializedRowTmp.getFields().get(1);

                    assertThat(valueCacheStartedAfter)
                            .as("different result for query=%s", query)
                            .isGreaterThan(valueCacheStartedBefore);
                    assertThat(valueCacheStartedAfter)
                            .as("different result for query=%s", query)
                            .isEqualTo(valueCacheAccomplishedAfter);
                    assertThat(valueCacheAccomplishedAfter)
                            .as("different result for query=%s", query)
                            .isGreaterThan(valueCacheAccomplishedBefore);
                });
    }

    /**
     * warm table_1, table_2 with default warming.
     * int_1- DATA, BASIC. v1- DATA, BASIC, LUCENE
     */
    private void prepare()
    {
        createTable(DEFAULT_SCHEMA,
                TABLE_1,
                "(" + COL_INT_1 + " integer, " + COL_V_1 + " varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO %s VALUES (1, 'shlomi'), (2, 'kobi')".formatted(TABLE_1));
        createTable(DEFAULT_SCHEMA,
                TABLE_2,
                "(" + COL_INT_1 + " integer, " + COL_V_1 + " varchar(20)) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), "INSERT INTO %s VALUES (3, 'roman'), (4, 'tal')".formatted(TABLE_2));

        Duration ttlDuration = Duration.ofHours(1);
        try {
            createWarmupRules(DEFAULT_SCHEMA,
                    TABLE_1,
                    Map.of(COL_INT_1,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 1, ttlDuration)),
                            COL_V_1,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 1, ttlDuration),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, 2, ttlDuration))));
            createWarmupRules(DEFAULT_SCHEMA,
                    TABLE_2,
                    Map.of(COL_INT_1,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 1, ttlDuration)),
                            COL_V_1,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 1, ttlDuration),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, 2, ttlDuration))));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        Session session = Session.builder(getSession())
                .setSystemProperty("cache_enabled", "false")
                .setSystemProperty(catalog + "." + WarpSessionProperties.ENABLE_DEFAULT_WARMING, "true")
                .build();

        //warm int_1 (DATA, BASIC), v1 (DATA, BASIC, LUCENE)
        String query = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_1 + " where " + COL_INT_1 + " > 0 and " + COL_V_1 + " like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query, session, 6, 3, 0);

        String query2 = "select " + COL_INT_1 + ", " + COL_V_1 + " from " + TABLE_2 + " where " + COL_INT_1 + " > 0 and " + COL_V_1 + " like '%shlomi%' and v1 > 's' and upper(v1) = 'SHLOMI'";
        warmAndValidate(query2, session, 6, 3, 0);
    }

    private void prepareCacheMgrRules(List<String> signatureKeys)
            throws IOException
    {
        List<CacheManagerRule> cacheManagerRules = signatureKeys.stream()
                .map(signatureKey -> new CacheManagerRule(
                        signatureKey,
                        10,
                        Duration.ofHours(1)))
                .toList();

        if (cacheMgrFetcherPath.toFile().exists()) {
            Path cacheRulesPath = getCacheRulesPath();
            if (cacheRulesPath.toFile().exists()) {
                logger.info("deleteCacheRulesFile::delete before write -> %s", cacheRulesPath);
                deleteCacheRulesFile(cacheRulesPath);
            }
            Files.write(
                    cacheRulesPath,
                    CompressionUtil.compressGzip(objectMapper.writeValueAsString(cacheManagerRules)));
            if (cacheRulesPath.toFile().exists()) {
                logger.info("written cache rules to %s", cacheRulesPath);
            }
            else {
                throw new RuntimeException("failed to create/write to file " + cacheRulesPath);
            }
        }
        else {
            throw new RuntimeException("cacheMgrFetcherPath doesnt exit -> " + cacheMgrFetcherPath);
        }

        String result = executeRestCommand(
                CacheMgrWarmupTask.CACHE_MANAGER_WARMUP_PATH,
                CacheMgrWarmupTask.TASK_NAME_FETCH,
                null,
                HttpMethod.GET,
                coordinatorPort,
                HttpURLConnection.HTTP_OK,
                false);
        Map<String, List<CacheManagerRule>> res = objectMapper
                .readerFor(new TypeReference<Map<String, List<CacheManagerRule>>>() {})
                .readValue(result);
        assertThat(res.values().stream().map(List::size).distinct().count())
                .isEqualTo(1);
        assertThat(res.values().stream().map(List::size).distinct().findFirst().orElseThrow())
                .isEqualTo(cacheManagerRules.size());
    }

    private Path getCacheRulesPath()
    {
        return Path.of(cacheMgrFetcherPath.toAbsolutePath().toString(), "rules");
    }

    private void deleteCacheRulesFile(Path cacheRulesPath)
    {
        FileUtils.deleteQuietly(cacheRulesPath.toFile());
    }

    private static QueryInfo getFullQueryInfo(DistributedQueryRunner queryRunner, MaterializedResultWithPlan materializedResultWithPlan)
    {
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(materializedResultWithPlan.queryId());
    }
}
