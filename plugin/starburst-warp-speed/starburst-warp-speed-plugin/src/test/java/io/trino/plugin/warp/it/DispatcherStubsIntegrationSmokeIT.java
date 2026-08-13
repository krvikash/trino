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
package io.trino.plugin.warp.it;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.it.DispatcherQueryRunner.configDefaultTtlInSeconds;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

// TODO - there seems to be a problem with running warmup with multiple workers on IT (we get an NPE)
public abstract class DispatcherStubsIntegrationSmokeIT
        extends DispatcherAbstractTestQueryFramework
{
    protected static final String WARP_MATCH_COLUMNS_STAT = "warp_match_columns";
    protected static final String WARP_COLLECT_COLUMNS_STAT = "warp_collect_columns";
    protected static final String PREFILLED_COLUMNS_STAT = "warp_prefilled_collect_columns";
    protected static final String EXTERNAL_MATCH_STAT = "external_match_columns";
    protected static final String EXTERNAL_COLLECT_STAT = "external_collect_columns";
    protected static final String WARP_MATCH_ON_SIMPLIFIED_DOMAIN_STAT = "warp_match_on_simplified_domain";
    protected static final String CACHED_TOTAL_ROWS = "cached_total_rows";
    protected static final String C1 = "int1";
    protected static final String C2 = "v1";
    protected static final double DEMOTE_CLEAN_UP_USAGE = 0;
    protected static final double DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT = 90;
    protected static final double DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT = 80;
    protected static final int DEMOTE_DEFAULT_BATCH_SIZE = 1;
    protected static final int DEMOTE_DEFAULT_EPSILON = 1;
    protected static final int DEMOTE_MAX_ELEMENTS_TO_DEMOTE = 1;
    private static final Logger logger = Logger.get(DispatcherStubsIntegrationSmokeIT.class);
    public static final List<String> DEMOTE_JMX_NAMES = List.of("number_of_runs", "number_of_runs_fail", "not_executed_due_threshold", "not_executed_due_is_already_executing");
    protected final int numNodes;
    protected final StubsStorageEngine stubsStorageEngine;
    protected final WarpStubsStorageEngineModule storageEngineModule;
    protected final String catalog;
    protected final boolean isWarpExtensionModule;

    public DispatcherStubsIntegrationSmokeIT(int numNodes, String catalog)
    {
        this(numNodes, catalog, true);
    }

    public DispatcherStubsIntegrationSmokeIT(int numNodes, String catalog, boolean isWarpExtensionModule)
    {
        this.numNodes = numNodes;
        this.catalog = catalog;
        this.isWarpExtensionModule = isWarpExtensionModule;
        storageEngineModule = new WarpStubsStorageEngineModule();
        stubsStorageEngine = (StubsStorageEngine) storageEngineModule.getStorageEngine();
    }

    @BeforeEach
    @Override
    public void beforeMethod(TestInfo testInfo)
    {
        super.beforeMethod(testInfo);
        createSchemaAndTable(DEFAULT_SCHEMA, "t", format("(%s integer, %s varchar(20))", C1, C2));
    }

    @AfterEach
    @Override
    public void afterMethod(TestInfo testInfo)
    {
        runWithRetries(() -> {
            try {
                if (isWarpExtensionModule) {
                    demoteAll();
                    executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_RESET_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
                    executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(), HttpMethod.POST, HttpURLConnection.HTTP_OK);
                    cleanWarmupRules();
                }

                try {
                    createdSchemas.forEach(schemaName -> {
                        MaterializedResult materializedRows = computeActual("show tables from " + schemaName);
                        if (!materializedRows.getMaterializedRows().isEmpty()) {
                            for (MaterializedRow materializedRow : materializedRows.getMaterializedRows()) {
                                assertUpdate("DROP TABLE IF EXISTS %s.%s".formatted(schemaName, materializedRow.getField(0)));
                            }
                        }
                        try {
                            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
                        }
                        catch (QueryFailedException e) {
                            if (!e.getMessage().endsWith("does not exist")) {
                                throw e;
                            }
                        }
                    });
                    createdSchemas.clear();
                }
                catch (Throwable e) {
                    logger.error(e, "failed on drop schema");
                }
            }
            catch (Throwable e) {
                fail(e.getMessage());
            }
        });
        super.afterMethod(testInfo);
    }

    protected void demoteAll()
    {
        demoteAll(Target.COORDINATOR);
    }

    protected void demoteAll(Target target)
    {
        try {
            logger.debug("demote all start");

            String result = executeRestCommand(
                    WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                    WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                    WarmupDemoterData.builder().maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                            .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                            .executeDemoter(true)
                            .modifyConfig(true)
                            .resetHighestPriority(true)
                            .forceDeleteFailedObjects(true)
                            .defaultRuleTtlInSeconds(configDefaultTtlInSeconds)
                            .build(),
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_OK,
                    target);

            Map<String, Object> res = jsonMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(result);
            Double highestPriority = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.HIGHEST_PRIORITY_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            logger.debug("demote task finish =" + res);
            assertThat(highestPriority).isEqualTo(0);

            restDemoteConfigToDefaults(target);

//            validateEmptyUsage();
            logger.debug("demote all finish");
        }
        catch (IOException e) {
            fail("demote failed", e);
        }
    }

    protected void restDemoteConfigToDefaults()
    {
        restDemoteConfigToDefaults(Target.COORDINATOR);
    }

    protected void restDemoteConfigToDefaults(Target target)
    {
        try {
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT)
                    .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT)
                    .batchSize(DEMOTE_DEFAULT_BATCH_SIZE)
                    .epsilon(DEMOTE_DEFAULT_EPSILON)
                    .maxElementsToDemoteInIteration(DEMOTE_MAX_ELEMENTS_TO_DEMOTE)
                    .forceExecuteDeadObjects(false)
                    .modifyConfig(true)
                    .resetHighestPriority(true)
                    .executeDemoter(false)
                    .defaultRuleTtlInSeconds(configDefaultTtlInSeconds)
                    .build();
            Map<String, Object> res = jsonMapper.readerFor(new TypeReference<Map<String, Object>>() {})
                    .readValue(executeRestCommand(
                            WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                            WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                            warmupDemoterData,
                            HttpMethod.POST,
                            HttpURLConnection.HTTP_OK,
                            target));
            Double maxUsage = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.MAX_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double cleanUsage = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.CLEANUP_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer batchSize = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.BATCH_SIZE_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double epsilon = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.EPSILON_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer maxElementsToDemote = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();

            assertThat(maxUsage).isEqualTo(DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT);
            assertThat(cleanUsage).isEqualTo(DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT);
            assertThat(batchSize).isEqualTo(DEMOTE_DEFAULT_BATCH_SIZE);
            assertThat(epsilon).isEqualTo(DEMOTE_DEFAULT_EPSILON);
            assertThat(maxElementsToDemote).isEqualTo(DEMOTE_MAX_ELEMENTS_TO_DEMOTE);
        }
        catch (IOException e) {
            logger.error(e);
        }
    }

    protected Session createJmxSession()
    {
        return Session.builder(getSession())
                .setCatalog("jmx")
                .setSchema("current")
                .build();
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, boolean filterRange)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), OptionalInt.empty(), OptionalInt.empty(), filterRange);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), OptionalInt.empty(), OptionalInt.empty(), false);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, OptionalInt expectedSplits)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), expectedSplits, OptionalInt.empty(), false);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, OptionalInt expectedSplits, OptionalInt expectedRows)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), expectedSplits, expectedRows, false);
    }

    protected void validateQueryStats(
            @Language("SQL") String query,
            Session session,
            Map<String, Long> expectedJmxCounters,
            Collection<String> expectedPositiveQueryStats)
    {
        validateQueryStats(query, session, expectedJmxCounters, expectedPositiveQueryStats, OptionalInt.empty(), OptionalInt.empty(), false);
    }

    protected void validateQueryStats(
            @Language("SQL") String query,
            Session session,
            Map<String, Long> expectedJmxCounters,
            Collection<String> expectedPositiveQueryStats,
            OptionalInt expectedSplits,
            OptionalInt expectedRows,
            boolean filerRange)
    {
        Session jmxSession = createJmxSession();
        List<String> jmxCounters = expectedJmxCounters.keySet().stream().toList();
        StringJoiner columnJoiner = new StringJoiner(", ");
        for (String jmxCounter : jmxCounters) {
            columnJoiner.add(String.format("sum(%s) as %s", jmxCounter, jmxCounter));
        }
        @Language("SQL") String jmxQuery = format("select %s from \"*DispatcherPageSource*\"", columnJoiner);
        MaterializedResult jmxBefore = null;
        if (!jmxCounters.isEmpty()) {
            jmxBefore = computeActual(jmxSession, jmxQuery);
        }

        if (!filerRange) {
            session = Session.builder(session)
                    .setSystemProperty(catalog + "." + WarpSessionProperties.MIN_MAX_FILTER, "false")
                    .build();
        }
        QueryRunner.MaterializedResultWithPlan resultWithQueryId = getQueryRunner().executeWithPlan(
                session,
                query);
        if (expectedSplits.isPresent()) {
            assertThat(resultWithQueryId.result()
                    .getStatementStats()
                    .orElseThrow()
                    .getTotalSplits())
                    .as("different result for total splits ")
                    .isEqualTo(expectedSplits.getAsInt());
        }
        if (expectedRows.isPresent()) {
            assertThat(resultWithQueryId.result().getRowCount())
                    .as("different result for row count ")
                    .isEqualTo(expectedRows.getAsInt());
        }
        Map<String, Long> customMetrics = getCustomMetrics(resultWithQueryId.queryId(), (DistributedQueryRunner) getQueryRunner());
        for (Map.Entry<String, Long> expectedStat : expectedJmxCounters.entrySet()) {
            String key = DispatcherPageSourceFactory.createFixedStatKey(DispatcherPageSourceStats.createKey(), expectedStat.getKey());
            Long actualResult = customMetrics.getOrDefault(key, 0L);
            Long expectedResult = expectedStat.getValue();
            assertThat(actualResult)
                    .as("stat: %s, actualResult: %d, expectedResult: %d. query: %s", key, actualResult, expectedResult, query)
                    .isEqualTo(expectedResult);
        }
        for (String stat : expectedPositiveQueryStats) {
            // STATS_DISPATCHER_KEY is default
            String key = stat.contains(":") ? stat : DispatcherPageSourceFactory.createFixedStatKey(DispatcherPageSourceStats.createKey(), stat);
            Long actualResult = customMetrics.get(key);
            assertThat(actualResult)
                    .as("stat: %s, actualResult: %d, expectedResult: >0. query: %s", key, actualResult, query)
                    .isGreaterThan(0);
        }
        if (!jmxCounters.isEmpty()) {
            MaterializedResult jmxAfter = computeActual(jmxSession, jmxQuery);
            List<MaterializedRow> materializedRows = jmxBefore.getMaterializedRows();
            assertThat(materializedRows.size()).isOne();
            MaterializedRow statBefore = materializedRows.getFirst();
            MaterializedRow statAfter = jmxAfter.getMaterializedRows().getFirst();
            List<Object> fields = statBefore.getFields();
            for (int j = 0; j < jmxCounters.size(); j++) {
                String counterName = jmxCounters.get(j);
                Object valueBefore = fields.get(j);
                Object valueAfter = statAfter.getField(j);
                assertThat((Long) valueAfter - (Long) valueBefore)
                        .as("different result for stat=%s. query=%s", counterName, query)
                        .isEqualTo(expectedJmxCounters.get(counterName));
            }
        }
    }

    public static Map<String, Long> getCustomMetrics(QueryId queryId, DistributedQueryRunner queryRunner)
    {
        QueryInfo info = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);
        Metrics.Accumulator metrics = Metrics.accumulator();
        info.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics).forEach(metrics::add);
        return metrics.get()
                .getMetrics()
                .entrySet()
                .stream()
                .filter(metricEntry -> metricEntry.getValue() instanceof LongCount)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ((Count<?>) entry.getValue()).getTotal()));
    }

    @SuppressWarnings("LanguageMismatch")
    protected void warmAndValidate(String query, Session session, String warmValidationStat, int rowCount)
    {
        Session jmxSession = createJmxSession();
        int beforeStats = getWarmingServiceStats(jmxSession, warmValidationStat);
        MaterializedResult materializedRows = computeActual(session, query);
        validateLazyWarming(beforeStats, warmValidationStat);
        assertThat(materializedRows.getRowCount()).isEqualTo(rowCount);
    }

    protected void warmAndValidate(String query, boolean defaultWarmup, String warmValidationStat, int rowCount)
    {
        Session session = Session.builder(getSession()).build();
        warmAndValidate(query, session, warmValidationStat, rowCount);
    }

    @SuppressWarnings("LanguageMismatch")
    protected void warmAndValidate(
            String query,
            Session session,
            int expectedFinishedWarmupElements,
            int expectedWarmAccomplished,
            Integer expectedWarmedFailed)
    {
        warmAndValidate(
                query,
                session,
                expectedFinishedWarmupElements,
                expectedWarmAccomplished,
                Optional.ofNullable(expectedWarmedFailed));
    }

    protected void warmAndValidate(
            @Language("SQL") String query,
            Session session,
            int expectedFinishedWarmupElements,
            int expectedWarmAccomplished,
            Optional<Integer> expectedWarmedFailed)
    {
        List<String> statsColNames = List.of(
                "warm_accomplished",
                "warmup_elements_count",
                "warm_failed",
                "warm_started");
        String warmStatsTableName = "%s:catalog=%s,name=%s_%s_*,type=%s".formatted(
                WarmingServiceStats.class.getPackageName(),
                catalog,
                WarmingServiceStats.createKey(),
                catalog,
                WarmingServiceStats.class.getSimpleName().toLowerCase(Locale.ROOT));
        Session jmxSession = createJmxSession();
        MaterializedRow materializedRow = null;
        try {
            materializedRow = getServiceStats(jmxSession, warmStatsTableName, statsColNames);
        }
        catch (Throwable e) {
            MaterializedResult rows = computeActual(createJmxSession(), "show tables");
            logger.warn(e,
                    "jmx[%d] tables: %s",
                    rows.getMaterializedRows().size(),
                    rows.getMaterializedRows()
                            .stream()
                            .filter(materializedRowTmp -> ((String) materializedRowTmp.getField(0)).contains(WarmingServiceStats.createKey()))
                            .collect(Collectors.toList()));
            return;
        }
        long beforeWarmAccomplishedStats = (Long) requireNonNull(materializedRow).getField(0);
        long beforeWarmupElementsCount = (Long) materializedRow.getField(1);
        long beforeWarmupFailedCount = (Long) materializedRow.getField(2);
        computeActual(session, query);

        runWithRetries(() -> {
            MaterializedRow materializedRowAfter = getServiceStats(jmxSession, warmStatsTableName, statsColNames);
            logger.debug("beforeWarmAccomplishedStats=%d, materializedRowAfter.getField(0)=%s", beforeWarmAccomplishedStats, materializedRowAfter.getField(0));
            long actualWarmAccomplished = (Long) materializedRowAfter.getField(0) - beforeWarmAccomplishedStats;
            long actualElementsFinishedCount = (Long) materializedRowAfter.getField(1) - beforeWarmupElementsCount;
            long actualWarmFailed = (Long) materializedRowAfter.getField(2) - beforeWarmupFailedCount;
            logger.debug(
                    "actualWarmAccomplished=%d, expectedWarmAccomplished=%d, actualElementsFinishedCount=%d, expectedFinishedWarmupElements=%d, actualWarmFailed=%d, expectedWarmedFailed=%s",
                    actualWarmAccomplished,
                    expectedWarmAccomplished,
                    actualElementsFinishedCount,
                    expectedFinishedWarmupElements,
                    actualWarmFailed,
                    expectedWarmedFailed.toString());
            assertThat(actualWarmAccomplished)
                    .describedAs("actualWarmAccomplished is not as expected. %s", query)
                    .isEqualTo(expectedWarmAccomplished);
            assertThat(actualElementsFinishedCount)
                    .describedAs("expectedFinishedWarmupElements is not as expected. %s", query)
                    .isEqualTo(expectedFinishedWarmupElements);

            expectedWarmedFailed.ifPresent(integer -> assertThat(actualWarmFailed)
                    .describedAs("expectedWarmedFailed is not as expected. %s", query)
                    .isEqualTo(integer.longValue()));
        });
    }

    protected void warmAndValidate(
            String query,
            boolean defaultWarmup,
            int expectedWarmupElements,
            int expectedWarmFinished)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + WarpSessionProperties.ENABLE_DEFAULT_WARMING_INDEX, "false")
                .setSystemProperty(catalog + "." + WarpSessionProperties.EMPTY_QUERY, "true")
                .build();
        warmAndValidate(
                query,
                session,
                expectedWarmupElements,
                expectedWarmFinished,
                0);
    }

    protected void warmAndValidateWithExport(
            String query,
            Session session,
            int expectedWarmupElements,
            int expectedWarmFinished,
            int expectedExportRowGroupsAccomplished)
    {
        Session jmxSession = createJmxSession();
        MaterializedResult jmxBefore = computeActual(jmxSession, "select sum(export_row_group_accomplished) from \"*warmupExportService*\"");
        long beforeExportRowGroupCount = (long) (Long) jmxBefore.getMaterializedRows().getFirst().getField(0);

        warmAndValidate(query, session, expectedWarmupElements, expectedWarmFinished, 0);

        runWithRetries(() -> {
            MaterializedResult jmxAfter = computeActual(jmxSession, "select sum(export_row_group_accomplished) from \"*warmupExportService*\"");
            MaterializedRow materializedRowAfter = jmxAfter.getMaterializedRows().getFirst();
            long actualNewExportRowGroupAccomplishedAfter = (Long) materializedRowAfter.getField(0) - beforeExportRowGroupCount;
            logger.info(
                    "actualNewExportRowGroupAccomplished=%d, expectedExportRowGroupsAccomplished=%d",
                    actualNewExportRowGroupAccomplishedAfter,
                    expectedExportRowGroupsAccomplished);
            assertThat(actualNewExportRowGroupAccomplishedAfter).isEqualTo(expectedExportRowGroupsAccomplished);
        });
    }

    protected void validateLazyWarming(int beforeWarmStats, String warmValidationStat)
    {
        Session jmxSession = createJmxSession();

        runWithRetries(() -> {
            long numberOfWarmProcessedFiles = getWarmingServiceStats(jmxSession, warmValidationStat);
            assertThat(numberOfWarmProcessedFiles).isGreaterThan(beforeWarmStats);
        });
    }

    @SuppressWarnings("SameParameterValue")
    protected void warmAndValidateLazyDemote(String query, boolean lazyWarmup)
    {
        Session jmxSession = createJmxSession();
        String jmxTable = WarmupDemoterStats.createKey();
        MaterializedRow before = getServiceStats(jmxSession, jmxTable, DEMOTE_JMX_NAMES);
        warmAndValidate(query, lazyWarmup, "warm_finished", 1);

        runWithRetries(() -> assertThat(validateStat(before, jmxTable, DEMOTE_JMX_NAMES)).isTrue());
    }

    protected void validateDemoter(int expectedDeadObjects)
            throws IOException
    {
        validateDemoter(Target.COORDINATOR, new DemoteInput(catalog, expectedDeadObjects, 0));
    }

    protected void validateDemoter(Target target, DemoteInput... demoteInputs)
            throws IOException
    {
        assertThat(getRowGroupCount(target).nodesWarmupElementsCount()
                .values()
                .stream()
                .reduce(0L, Long::sum))
                .isPositive();

        Session jmxSession = createJmxSession();

        restDemoteConfigToDefaults(target);

        List<String> demoteColumns = List.of("dead_objects_deleted", "deleted_by_low_priority");

        Map<String, DemoteInput> demoteInputsBefore = Arrays.stream(demoteInputs).map(demoteInput -> {
            String jmxTable = "%s:*name=%s_%s,type=%s".formatted(
                    WarmupDemoterStats.class.getPackageName(),
                    WarmupDemoterStats.createKey(),
                    demoteInput.catalog(),
                    WarmupDemoterStats.class.getSimpleName().toLowerCase(Locale.ROOT));
            MaterializedRow materializedRow = getServiceStats(jmxSession, jmxTable, demoteColumns);
            return new DemoteInput(demoteInput.catalog(), (Long) materializedRow.getField(0), (Long) materializedRow.getField(1));
        }).collect(Collectors.toMap(DemoteInput::catalog, Function.identity()));

        executeRestCommand(
                WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                WarmupDemoterData.builder()
                        .maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                        .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                        .executeDemoter(true)
                        .modifyConfig(true)
                        .resetHighestPriority(true)
                        .forceDeleteFailedObjects(true)
                        .forceExecuteDeadObjects(true)
                        .defaultRuleTtlInSeconds(configDefaultTtlInSeconds)
                        .build(),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK,
                target);

        runWithRetries(() -> {
            Map<String, DemoteInput> demoteInputsAfter = Arrays.stream(demoteInputs)
                    .map(demoteInput -> {
                        String jmxTable = "%s:*name=%s_%s,type=%s".formatted(
                                WarmupDemoterStats.class.getPackageName(),
                                WarmupDemoterStats.createKey(),
                                demoteInput.catalog(),
                                WarmupDemoterStats.class.getSimpleName().toLowerCase(Locale.ROOT));
                        MaterializedRow materializedRow = getServiceStats(jmxSession, jmxTable, demoteColumns);
                        return new DemoteInput(demoteInput.catalog(), (Long) materializedRow.getField(0), (Long) materializedRow.getField(1));
                    }).collect(Collectors.toMap(DemoteInput::catalog, Function.identity()));

            Arrays.stream(demoteInputs).forEach(demoteInput -> {
                String catalog = demoteInput.catalog();
                DemoteInput demoteInputBefore = demoteInputsBefore.get(catalog);
                DemoteInput demoteInputAfter = demoteInputsAfter.get(catalog);
                long actualDeadObjectCount = demoteInputAfter.deadObjects() - demoteInputBefore.deadObjects();
                long actualDeletedByLowPriority = demoteInputAfter.deletedByLowPriority() - demoteInputBefore.deletedByLowPriority();
                assertThat(actualDeadObjectCount)
                        .describedAs(catalog + " -> DeadObjects")
                        .isEqualTo(demoteInput.deadObjects());
                assertThat(actualDeletedByLowPriority)
                        .describedAs(catalog + " -> DeletedByLowPriority")
                        .isEqualTo(demoteInput.deletedByLowPriority());
            });
        });

        assertThat(getRowGroupCount(target).nodesWarmupElementsCount()
                .values()
                .stream()
                .reduce(0L, Long::sum))
                .isZero();
    }

    protected Map<String, Object> demote(WarmupDemoterData warmupDemoterData)
            throws IOException
    {
        restDemoteConfigToDefaults();
        Session jmxSession = createJmxSession();
        String jmxTable = WarmupDemoterStats.createKey();
        MaterializedRow before = getServiceStats(jmxSession, jmxTable, DEMOTE_JMX_NAMES);

        String result = executeRestCommand(
                WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                warmupDemoterData,
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK,
                Target.COORDINATOR);
        Map<String, Object> res = jsonMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(result);
        if (warmupDemoterData.isExecuteDemoter()) {
            boolean valid = validateStat(before, jmxTable, DEMOTE_JMX_NAMES);
            assertThat(valid).isTrue();
        }
        return res;
    }

    protected boolean validateStat(
            MaterializedRow beforeStatsMaterializedRow,
            String jmxTable,
            List<String> statColNames)
    {
        Session jmxSession = createJmxSession();
        MaterializedRow afterStatsMaterializedRow = getServiceStats(jmxSession, jmxTable, statColNames);
        boolean result = true;
        for (int i = 0; i < beforeStatsMaterializedRow.getFieldCount(); i++) {
            result = (long) beforeStatsMaterializedRow.getField(i) <= (long) afterStatsMaterializedRow.getField(i);
            if (!result) {
                logger.warn(
                        "validateStat:: before[%s]= %s VS after[%s]=%s",
                        statColNames.get(i),
                        beforeStatsMaterializedRow.getField(i),
                        statColNames.get(i),
                        afterStatsMaterializedRow.getField(i));
                break;
            }
        }
        return result;
    }

    protected int getWarmingServiceStats(Session jmxSession, String statColName)
    {
        String warmStatsTableName = "%s:catalog=%s,name=%s_%s_*,type=%s".formatted(
                WarmingServiceStats.class.getPackageName(),
                catalog,
                WarmingServiceStats.createKey(),
                catalog,
                WarmingServiceStats.class.getSimpleName().toLowerCase(Locale.ROOT));

        long result = (long) getServiceStats(
                jmxSession,
                warmStatsTableName,
                List.of(statColName))
                .getField(0);
        return (int) result;
    }

    protected MaterializedRow getServiceStats(Session jmxSession, String jmxTable, List<String> statColNames)
    {
        String statSumColNames = statColNames.stream()
                .map(s -> "sum(" + s + ")")
                .collect(Collectors.joining(","));
        try {
            MaterializedResult jmx0 = computeActual(jmxSession, String.format("select %s from \"*%s*\"", statSumColNames, jmxTable));
            logger.debug("getServiceStats::jmxTable=%s", jmxTable);
            return jmx0.getMaterializedRows().getFirst();
        }
        catch (Throwable e) {
            MaterializedResult rows = computeActual(createJmxSession(), "show tables");
            logger.error(e,
                    "jmx[%d] tables: %s",
                    rows.getMaterializedRows().size(),
                    rows.getMaterializedRows()
                            .stream()
                            .filter(materializedRowTmp -> ((String) materializedRowTmp.getField(0)).contains(WarmingServiceStats.createKey()))
                            .collect(Collectors.toList()));
            throw new RuntimeException("failed");
        }
    }

    protected void runWithRetries(Runnable runnable)
    {
        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionError.class)
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(2))
                        .build())
                .run(runnable::run);
    }

    protected Session buildSession(boolean defaultWarm, boolean enableDefaultWarmIndex)
    {
        Session.SessionBuilder sessionBuilder = Session.builder(getSession());
        if (defaultWarm) {
            if (enableDefaultWarmIndex) {
                sessionBuilder.setSystemProperty(catalog + "." + WarpSessionProperties.ENABLE_DEFAULT_WARMING_INDEX, "true");
            }
        }
        return sessionBuilder.build();
    }

    public record DemoteInput(String catalog, long deadObjects, long deletedByLowPriority) {}
}
