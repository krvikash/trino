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
package io.trino.tests.product.warp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.api.warmup.column.RegularColumnData;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.JMXCachingConstants;
import io.trino.tests.product.warp.utils.JMXCachingManager;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestCasesFormat;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.WarmUtils;
import io.trino.tests.product.warp.utils.syntheticconfig.TableType;
import org.intellij.lang.annotations.Language;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_FAILED;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getValue;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestDemote
{
    private static final Logger logger = Logger.get(TestDemote.class);
    public static final String SCHEMA_NAME = "synthetic";

    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;
    @Inject
    RestUtils restUtils;

    public TestDemote() {}

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        ruleUtils.resetAllRules(RestUtils.CATALOG_1_PORT);
    }

    @AfterMethodWithContext
    public void after() {}

    @DataProvider
    public Iterator<TestFormat> syntheticWarp(ITestContext context)
            throws Exception
    {
        return executeDataProvider(
                "file:///docker/trino-product-tests/warp/synthetic_matrix.json",
                TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE, PROFILE_SPECIFIC_TESTS}, dataProvider = "syntheticWarp")
    public void testDemoteDefaultWarmup(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp, true, 0, true);
    }

    @DataProvider
    public Iterator<TestCasesFormat> warmupDemoter(ITestContext context)
            throws Exception
    {
        return executeCasesDataProvider("file:///docker/trino-product-tests/warp/warmup_demoter.json");
    }

    @Test(groups = {WARP_SPEED_HIVE, PROFILE_SPECIFIC_TESTS}, dataProvider = "warmupDemoter")
    public void testDemotePriorityWarmup(TestCasesFormat testCasesFormat)
    {
        execute(testCasesFormat);
    }

    private Iterator<TestFormat> executeDataProvider(String filePath, TableType tableType)
            throws Exception
    {
        logger.info("running %s", filePath);
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new URI(filePath).toURL());
        return tests.stream()
                .map(testFormat -> TestFormat.builder(testFormat).build(tableType.name()))
                .filter(TestFormat::pt_enable)
                .filter(testFormat -> !testFormat.skip())
//                .filter(testFormat -> (testFormat.skip_type() == null) || !testFormat.skip_type().contains(tableType))
//                .map(testFormat -> updateTableType(testFormat, tableType))
                .iterator();
    }

    private Iterator<TestCasesFormat> executeCasesDataProvider(String filePath)
            throws Exception
    {
        logger.info("running %s", filePath);
        List<TestCasesFormat> tests = objectMapper.readerFor(new TypeReference<List<TestCasesFormat>>() {})
                .readValue(new URI(filePath).toURL());
        return tests.stream()
//                .map(testFormat -> TestFormat.builder(testFormat).build(tableType.name()))
                .filter(testFormat -> !testFormat.skip())
                .filter(TestCasesFormat::pt_enable)
//                .filter(testFormat -> (testFormat.skip_type() == null) || !testFormat.skip_type().contains(tableType))
//                .map(testFormat -> updateTableType(testFormat, tableType))
                .iterator();
    }

    private void execute(TestCasesFormat testCasesFormat)
    {
        testCasesFormat.cases()
                .forEach(testCase -> {
                    logger.info("START test [%s]", testCase.case_info());
                    TestFormat testFormat = TestFormat.builder()
                            .name(testCase.case_info())
                            .tableName(testCasesFormat.getTableName())
                            .warmupRules(testCase.warmup_rules() != null ? testCase.warmup_rules()
                                    .stream()
                                    .map(warmupRule -> new TestFormat.WarmupRule(
                                            warmupRule.colNameId(),
                                            warmupRule.predicates(),
                                            List.of(warmupRule.warmUpType()),
                                            warmupRule.priority(),
                                            warmupRule.ttl()))
                                    .toList() : List.of())
                            .warmQuery(testCase.warm_query())
                            .queriesData(testCase.queries_data())
                            .structure(testCasesFormat.structure())
                            .partitionBy(testCasesFormat.partition_by())
                            .bucketedBy(testCasesFormat.bucketed_by())
                            .bucketCount(testCasesFormat.bucket_count())
                            .expectedWarmFailures(testCase.expected_warm_failures())
                            .failedWarmupElements(testCase.failed_warmup_elements())
                            .build();
                    try {
                        execute(testFormat,
                                TableType.warp,
                                testCase.default_warmup(),
                                testCase.sleep_time_for_ttl(),
                                false);

                        if ((testCase.expected_result() != null) && !testCase.expected_result().isEmpty()) {
                            QueryResult demoterStatsAfter = JMXCachingManager.getDemoterStats();
                            if ((Boolean) testCase.expected_result().getOrDefault(JMXCachingConstants.WarmupDemoter.DEAD_OBJECTS_DELETED, false)) {
                                assertThat(getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.DEAD_OBJECTS_DELETED))
                                        .describedAs("%s not demoted for case [%s]".formatted(
                                                JMXCachingConstants.WarmupDemoter.DEAD_OBJECTS_DELETED,
                                                testCase.case_info()))
                                        .isGreaterThan(0);
                            }
                            if ((Boolean) testCase.expected_result().getOrDefault(JMXCachingConstants.WarmupDemoter.DELETED_BY_LOW_PRIORITY, false)) {
                                assertThat(getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.DELETED_BY_LOW_PRIORITY))
                                        .describedAs("%s not demoted for case [%s]".formatted(
                                                JMXCachingConstants.WarmupDemoter.DELETED_BY_LOW_PRIORITY,
                                                testCase.case_info()))

                                        .isGreaterThan(0);
                            }

                            //try to warm with "highest_deleted_priority", should fail due to demote
                            if ((Integer) testCase.expected_result().getOrDefault("highest_deleted_priority", 0) > 0) {
                                TestFormat.Column column = testFormat.structure()
                                        .stream()
                                        .filter(columnTmp -> testFormat.warmup_rules()
                                                .stream()
                                                .filter(warmupRule -> warmupRule.colNameId() != null)
                                                .noneMatch(warmupRule -> warmupRule.colNameId().equals(columnTmp.name())))
                                        .findFirst()
                                        .orElseThrow(() -> new RuntimeException("cant find a column without rules"));

                                WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                                        SCHEMA_NAME,
                                        testFormat.getTableName(),
                                        new RegularColumnData(column.name()),
                                        WarmUpType.WARM_UP_TYPE_DATA,
                                        (int) testCase.expected_result().get("highest_deleted_priority"),
                                        Duration.ofSeconds(10),
                                        ImmutableSet.of());

                                ruleUtils.createRules(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, testFormat.getTableName(), Set.of(warmupColRuleData));

                                onTrino().executeQuery("USE warp.%s".formatted(SCHEMA_NAME));

                                QueryResult warmingStatsBefore = JMXCachingManager.getWarmingStats();
                                warmUtils.warmAndValidate(
                                        testFormat.warm_query(),
                                        Map.of(WARM_ACCOMPLISHED, 0L, WARM_FAILED, 0L));
                                QueryResult warmingStatsAfter = JMXCachingManager.getWarmingStats();

                                assertThat(getValue(warmingStatsAfter, JMXCachingConstants.WarmingService.STARTED))
                                        .describedAs("%s for case [%s]".formatted(JMXCachingConstants.WarmingService.STARTED, testCase.case_info()))
                                        .isEqualTo(getValue(warmingStatsBefore, JMXCachingConstants.WarmingService.STARTED));

                                assertThat(getValue(warmingStatsAfter, JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED))
                                        .describedAs("%s for case [%s]".formatted(JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED, testCase.case_info()))
                                        .isEqualTo(getValue(warmingStatsBefore, JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED));
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        logger.info("FINISH test [%s]", testCase.case_info());
                        try {
                            ruleUtils.resetTableRules(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, testFormat);
                            demoterUtils.demote(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, testFormat.getTableName(), testFormat);
                            demoterUtils.resetToDefaultDemoterConfiguration(RestUtils.CATALOG_1_PORT);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private void execute(
            TestFormat testFormat,
            TableType tableType,
            boolean defaultWarming,
            int sleepTimeForTtl,
            boolean reset)
            throws IOException
    {
        logger.info("starting run test %s", testFormat.name());
        String tableName = testFormat.getTableName();

        try (QueryExecutor queryExecutor = onTrino()) {
            createTable(testFormat, tableType, queryExecutor, SCHEMA_NAME, tableName);

            queryExecutor.executeQuery("USE warp.%s".formatted(SCHEMA_NAME));

            if (!defaultWarming) {
                ruleUtils.createWarmupRules(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, testFormat);
            }

            TestFormat newTestFormat = TestFormat.builder(testFormat)
                    .sessionProperties(Map.of("enable_default_warming", Boolean.toString(defaultWarming)))
                    .build();

            warmUtils.warmAndValidate(RestUtils.CATALOG_1_PORT, "warp", newTestFormat, FastWarming.NONE);

            QueryResult demoterStatsBefore = JMXCachingManager.getDemoterStats();

            long totalCapacity = getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.TOTAL_CAPACITY);
            long currentUsage = getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.CURRENT_USAGE);
            double demoteThreshold = ((((double) currentUsage / totalCapacity) * 100) - 2) / 100;

            int ttlInSeconds = 1;

            // wait for TTL to expire
            if (sleepTimeForTtl > 0) {
                Thread.sleep(Duration.ofSeconds(sleepTimeForTtl).toMillis());
            }
            else {
                Thread.sleep(Duration.ofSeconds(Integer.toUnsignedLong(ttlInSeconds * 2)).toMillis());
            }

            if (testFormat.expected_warm_failures() == 0) {
                //adjust the default TTL to 1 second so all default rules will be stale
                demoterUtils.demote(
                        WarmupDemoterData.builder()
                                .executeDemoter(true)
                                .modifyConfig(true)
                                .defaultRuleTtlInSeconds(ttlInSeconds)
                                .maxUsageThresholdInPercentage(demoteThreshold)
                                .cleanupUsageThresholdInPercentage(demoteThreshold)
                                .build(),
                        RestUtils.CATALOG_1_PORT,
                        false);

                QueryResult demoterStatsAfter = JMXCachingManager.getDemoterStats();

                assertThat(getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.FAILED_OBJECTS_DELETED))
                        .isEqualTo(getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.FAILED_OBJECTS_DELETED));
            }
            else {
                // in case of expected_warm_failures run manual demoter with default thresholds and forceDeleteFailedObjects=true to delete failed WE
                String string = restUtils.executeGetCommand(RestUtils.CATALOG_1_PORT, RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_TASK_NAME);
                RowGroupCountResult rowGroupCountResultBefore = objectMapper.readerFor(new TypeReference<RowGroupCountResult>() {}).readValue(string);

                demoterUtils.demote(
                        WarmupDemoterData.builder()
                                .executeDemoter(true)
                                .modifyConfig(true)
                                .forceDeleteFailedObjects(true)
                                .build(),
                        RestUtils.CATALOG_1_PORT,
                        false);

                QueryResult demoterStatsAfter = JMXCachingManager.getDemoterStats();

                assertThat(getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.FAILED_OBJECTS_DELETED))
                        .isEqualTo(getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.FAILED_OBJECTS_DELETED) + testFormat.expected_warm_failures());

                string = restUtils.executeGetCommand(RestUtils.CATALOG_1_PORT, RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_TASK_NAME);
                RowGroupCountResult rowGroupCountResultAfter = objectMapper.readerFor(new TypeReference<RowGroupCountResult>() {}).readValue(string);

                logger.info("############################### rowGroupCountResultBefore=%s", rowGroupCountResultBefore);
                logger.info("############################### rowGroupCountResultAfter=%s", rowGroupCountResultAfter);

                rowGroupCountResultBefore.warmupColumnNames().removeAll(rowGroupCountResultAfter.warmupColumnNames());
                assertThat(rowGroupCountResultBefore.warmupColumnNames()).isEqualTo(new HashSet<>(testFormat.failed_warmup_elements()));
            }
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw new RuntimeException(e);
        }
        finally {
            if (reset) {
                ruleUtils.resetTableRules(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, testFormat);
                demoterUtils.demote(RestUtils.CATALOG_1_PORT, SCHEMA_NAME, tableName, testFormat);
                demoterUtils.resetToDefaultDemoterConfiguration(RestUtils.CATALOG_1_PORT);
            }
        }
    }

    private void createTable(TestFormat testFormat,
            TableType tableType,
            QueryExecutor queryExecutor,
            String schemaName,
            String tableName)
    {
        queryExecutor.executeQuery("CREATE SCHEMA IF NOT EXISTS warp.%s".formatted(schemaName));

        queryExecutor.executeQuery("USE warp.%s".formatted(schemaName));

        @Language("SQL")
        String countSql = "select count(*) from warp.%s.%s".formatted(schemaName, tableName);

        if (!queryUtils.isTableExists(schemaName, tableName)) {
            @Language("SQL")
            String createTableSql = getCreateTableSql(schemaName, tableName, testFormat, tableType);
            logger.info(createTableSql);
            queryExecutor.executeQuery(createTableSql);

            if (!TableType.warp.equals(tableType)) {
                executeInsertTable(testFormat, schemaName, queryExecutor);
            }
        }
        else {
            // ensure that the dynamic catalog is loaded
            QueryResult queryResult = queryExecutor.executeQuery(countSql);
            // in case table was previously created but for some reason is empty
            if (!TableType.warp.equals(tableType)) {
                if (queryResult.getRowsCount() == 0) {
                    logger.info("table %s is empty, run insert query", tableName);
                    executeInsertTable(testFormat, schemaName, queryExecutor);
                }
            }
        }
        if ((testFormat.partition_by() != null) && !testFormat.partition_by().isEmpty()) {
            queryExecutor.executeQuery("CALL system.sync_partition_metadata('%s', '%s', 'FULL')"
                    .formatted(schemaName, tableName));
        }

//        try {
//            QueryResult showQueryResult = queryExecutor.executeQuery("show create table warp.synthetic.demoter_wix_16_table");
//            logger.info("################### showQueryResult.row(0)=" + showQueryResult.row(0));
//        }
//        catch (Throwable e) {
//            e.printStackTrace();
//        }

//        QueryResult showCreateTablequeryResult = queryExecutor.executeQuery("show create table warp.%s.%s".formatted(schemaName, tableName));
//        logger.info("#### [%s]", showCreateTablequeryResult.row(0));

        // fake query to ensure that the dynamic catalog is loaded
        QueryResult queryResult = queryExecutor.executeQuery(countSql);
        assertThat(queryResult.getRowsCount())
                .describedAs("Table[%s] is empty".formatted(tableName))
                .isGreaterThan(0);
        assertThat(((Long) queryResult.row(0).getFirst()))
                .describedAs("Table[%s] is empty".formatted(tableName))
                .isGreaterThan(0);
    }

    private String getCreateTableSql(String schemaName, String tableName, TestFormat testFormat, TableType tableType)
    {
        String columnNames = testFormat.structure()
                .stream()
                .map(column -> column.name() + " " + createColumnDefinition(column.type(), column.args()))
                .collect(Collectors.joining(","));
        String dataFormat = !tableType.equals(TableType.warp_delta_lake) ?
                "format='%s'".formatted(StringUtils.isNotEmpty(testFormat.data_format()) ? testFormat.data_format() : "PARQUET") :
                "";
        String partitionedByStr = (testFormat.partition_by() == null || testFormat.partition_by().isEmpty()) ? "" :
                "%s=ARRAY[%s]".formatted(
                        tableType.equals(TableType.warp_iceberg) ? IcebergTableProperties.PARTITIONING_PROPERTY : HiveTableProperties.PARTITIONED_BY_PROPERTY,
                        testFormat.partition_by()
                                .stream()
                                .map(s -> "'" + s + "'")
                                .collect(Collectors.joining(",")));

        String bucketedByStr = (testFormat.bucketed_by() == null || testFormat.bucketed_by().isEmpty()) ? "" :
                "bucketed_by=ARRAY[%s],bucket_count=%d".formatted(
                        testFormat.bucketed_by()
                                .stream()
                                .map(s -> "'" + s + "'")
                                .collect(Collectors.joining(",")),
                        testFormat.bucket_count());

        String location = "%s='s3://warp-speed-us-east1-systemtests/%s/%s'".formatted(
                tableType.equals(TableType.warp) ? HiveTableProperties.EXTERNAL_LOCATION_PROPERTY : IcebergTableProperties.LOCATION_PROPERTY,
                schemaName,
                tableName);

        return "CREATE TABLE IF NOT EXISTS warp.%s.%s (%s) WITH (%s)".formatted(
                schemaName,
                tableName,
                columnNames,
                Stream.of(dataFormat, location, partitionedByStr, bucketedByStr)
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(", ")));
    }

    private String createColumnDefinition(String fieldType, List<Object> args)
    {
        String fieldDef = fieldType;

        if (fieldType == null) {
            fieldDef = "integer";
        }
        else if (args == null || args.isEmpty()) {
            fieldDef = fieldType;
        }
        else if ("varchar".equals(fieldType)) {
            fieldDef = "varchar(%s)".formatted(args.getFirst());
        }
        else if ("array".equals(fieldType)) {
            if (args.size() == 1) {
                fieldDef = "array(%s)".formatted(args.getFirst());
            }
            else if (args.size() == 2 && "char".equals(args.get(0))) {
                fieldDef = "array(%s(%s))".formatted(args.get(0), args.get(1));
            }
            else {
                throw new RuntimeException("unknown field type %s, args %s".formatted(fieldType, args));
            }
        }
        else if ("map".equals(fieldType)) {
            fieldDef = "map(%s, %s)".formatted(args.get(0), args.get(1));
        }
        else if ("row".equals(fieldType)) {
            StringJoiner rowColumnDef = new StringJoiner(",", "(", ")");
            args.forEach(arg -> rowColumnDef.add(arg.toString()));
            fieldDef = "ROW" + rowColumnDef;
        }
        else if (fieldType.equals("char") || fieldType.equals("decimal")) {
            if (args.size() == 3) {
                args = args.subList(0, args.size() - 1);
            }
            else if (args.size() > 3) {
                throw new RuntimeException();
            }
            StringJoiner rowColumnDef = new StringJoiner(",", "(", ")");
            args.forEach(arg -> rowColumnDef.add(arg.toString()));
            fieldDef = fieldType + rowColumnDef;
        }
//        else {
//            fieldDef = fieldType;
//        }
        return fieldDef;
    }

    private void executeInsertTable(TestFormat testFormat, String schemaFrom, QueryExecutor queryExecutor)
    {
        @Language("SQL") String insertSql = "INSERT INTO %s select * from hive.%s.%s"
                .formatted(testFormat.getTableName(),
                        schemaFrom,
                        testFormat.orig_table_name().orElse(testFormat.getTableName()));
        logger.info(insertSql);
        queryExecutor.executeQuery(insertSql);
    }

//    private TestFormat updateTableType(TestFormat test, TableType tableType)
//    {
//        if (tableType == TableType.warp) {
//            return test;
//        }
//        String newTableName = test.table_name() == null ? test.name() : test.table_name();
//        return test.withTableType(newTableName, tableType);
//    }
}
