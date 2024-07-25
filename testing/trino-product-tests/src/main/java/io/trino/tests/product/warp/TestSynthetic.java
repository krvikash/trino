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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.WarmUtils;
import io.trino.tests.product.warp.utils.syntheticconfig.TableType;
import org.intellij.lang.annotations.Language;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_DELTA_LAKE;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_2;
import static io.trino.tests.product.TestGroups.WARP_SPEED_ICEBERG;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;

public class TestSynthetic
{
    private static final Logger logger = Logger.get(TestSynthetic.class);

    private static final String DELTA_TYPE_PREFIX = "dl_";
    private static final String ICEBERG_TYPE_PREFIX = "iceberg_";

    private final String formattedDateTime;

    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;

    public TestSynthetic()
    {
        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        formattedDateTime = now.format(formatter);
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        ruleUtils.resetAllRules();
    }

    @AfterMethodWithContext
    public void after() {}

    @DataProvider
    public Iterator<TestFormat> synthetic(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json", TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> syntheticDeltaLake(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json", TableType.warp_delta_lake);
    }

    @DataProvider
    public Iterator<TestFormat> syntheticIceberg(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json", TableType.warp_iceberg);
    }

    @Test(groups = {WARP_SPEED_HIVE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @Test(groups = {WARP_SPEED_DELTA_LAKE, PROFILE_SPECIFIC_TESTS}, dataProvider = "syntheticDeltaLake")
    public void syntheticDeltaLake(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp_delta_lake);
    }

    @Test(groups = {WARP_SPEED_ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "syntheticIceberg")
    public void syntheticIceberg(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp_iceberg);
    }

    @DataProvider
    public Iterator<TestFormat> lucene(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/lucene.json", TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "lucene")
    public void lucene(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> synthDict(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synth_dict.json", TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthDict")
    public void synthDict(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> synthPartit(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synth_partit.json", TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> synthPartitDelkalake(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synth_partit.json", TableType.warp_delta_lake);
    }

    @DataProvider
    public Iterator<TestFormat> synthPartitIceberg(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synth_partit.json", TableType.warp_iceberg);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthPartit")
    public void synthPartit(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @Test(groups = {WARP_SPEED_DELTA_LAKE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthPartitDelkalake")
    public void synthPartitDeltalake(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp_delta_lake);
    }

    @Test(groups = {WARP_SPEED_ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthPartitIceberg")
    public void synthPartitIceberg(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp_iceberg);
    }

    @DataProvider
    public Iterator<TestFormat> synthTypes(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synth_types.json", TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthTypes")
    public void synthTypes(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> synthMixed(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic_mix_query.json", TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthMixed")
    public void synthMixed(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
    }

    @DataProvider
    public Iterator<TestFormat> synthMatrix(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic_matrix.json", TableType.warp);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthMatrix")
    public void synthMatrix(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, TableType.warp);
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
                .filter(testFormat -> (testFormat.skip_type() == null) || !testFormat.skip_type().contains(tableType))
                .map(testFormat -> updateTableType(testFormat, tableType))
                .iterator();
    }

    private void execute(TestFormat testFormat, TableType tableType)
            throws IOException
    {
        logger.info("starting run test %s", testFormat.name());
        String origSchemaName = "synthetic";
        String schemaName = getTypePrefix(tableType) + origSchemaName;
        String tableName = testFormat.getTableName();

        try {
            QueryExecutor queryExecutor = onTrino();

            queryExecutor.executeQuery("CREATE SCHEMA IF NOT EXISTS warp.%s".formatted(schemaName));
            queryExecutor.executeQuery("USE warp.%s".formatted(schemaName));

            if (!queryUtils.isTableExists(schemaName, tableName)) {
                @Language("SQL") String createTableSql = getCreateTableSql(schemaName, tableName, testFormat, tableType);
                logger.info(createTableSql);
                queryExecutor.executeQuery(createTableSql);

                if (!TableType.warp.equals(tableType)) {
                    executeInsertTable(testFormat, origSchemaName, queryExecutor);
                }
            }
            else { // in case table was previously created but for some reason is empty
                @Language("SQL") String countSql = "select count(*) from %s".formatted(testFormat.getTableName());
                if (!TableType.warp.equals(tableType)) {
                    QueryResult queryResult = queryExecutor.executeQuery(countSql);
                    if (queryResult.getRowsCount() == 0) {
                        logger.info("table %s is empty, run insert query", tableName);
                        executeInsertTable(testFormat, origSchemaName, queryExecutor);
                    }
                }
            }

            queryExecutor.executeQuery("set session warp.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'".formatted(formattedDateTime));
            Map<String, Object> warpSessionProperties = testFormat.session_properties() != null ?
                    testFormat.session_properties().entrySet().stream().collect(Collectors.toMap(e -> "warp." + e.getKey(), Map.Entry::getValue)) :
                    Map.of();
            boolean defaultWarming = (boolean) warpSessionProperties.getOrDefault("warp.enable_default_warming", false);
            if (!defaultWarming) {
                ruleUtils.createWarmupRules(schemaName, testFormat);
            }
            warmUtils.setSessions(warpSessionProperties);

            boolean fastWarming = (boolean) warpSessionProperties.getOrDefault("warp.enable_import_export", false);
            if (fastWarming) {
                warmUtils.warmAndValidate(testFormat, FastWarming.EXPORT);
                demoterUtils.demote(schemaName, tableName, testFormat);
                demoterUtils.resetToDefaultDemoterConfiguration();
                warmUtils.warmAndValidate(testFormat, FastWarming.IMPORT);
            }
            else {
                warmUtils.warmAndValidate(testFormat, FastWarming.NONE);
            }
            warmUtils.resetSessions(warpSessionProperties);

            queryUtils.runQueries(testFormat);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw new RuntimeException(e);
        }
        finally {
            ruleUtils.resetTableRules(schemaName, testFormat);
            demoterUtils.demote(schemaName, tableName, testFormat);
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
    }

    private String getCreateTableSql(String schemaName, String tableName, TestFormat testFormat, TableType tableType)
    {
        String tableDefinition = testFormat.structure()
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
                tableDefinition,
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

    private static String getTypePrefix(TableType tableType)
    {
        return switch (tableType) {
            case warp_iceberg -> ICEBERG_TYPE_PREFIX;
            case warp_delta_lake -> DELTA_TYPE_PREFIX;
            default -> "";
        };
    }

    private TestFormat updateTableType(TestFormat test, TableType tableType)
    {
        if (tableType == TableType.warp) {
            return test;
        }
        String typePrefix = getTypePrefix(tableType);
        String newTableName = test.table_name() == null ? typePrefix + test.name() : typePrefix + test.table_name();
        return test.withTableType(newTableName, tableType);
    }
}
