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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.jdbc.TrinoResultSet;
import io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.syntheticconfig.ExcludeStrategy;
import jakarta.ws.rs.HttpMethod;
import org.testng.ITestContext;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.QUERY_ID;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.TEST_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWarpCache
{
    private static final Logger logger = Logger.get(TestSynthetic.class);
    private final String formattedDateTime;
    private boolean initialized;

    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;

    @Inject
    RestUtils restUtils;

    public TestWarpCache()
    {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        formattedDateTime = now.format(formatter);
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
                onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS warp.synthetic");
                onTrino().executeQuery("USE warp.synthetic");
                initialized = true;
            }
        }
        ruleUtils.resetAllRules();
    }

    @AfterMethodWithContext
    public void after()
    {
    }

    public static Iterator<Object[]> executeDataProvider(String filePath)
            throws Exception
    {
        logger.info("running %s", filePath);
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        return tests.stream()
                .filter(TestFormat::pt_enable)
                .map(x -> new Object[] {x})
                .iterator();
    }

    @DataProvider
    public Iterator<Object[]> cache(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/cache.json");
    }

    @DataProvider
    public Iterator<Object[]> synthTypes(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synth_types.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        String excludeString = "select count(*) from";
        HashMap<String, List<String>> configuration = excludeQueriesByPattern(excludeString, tests);
        List<String> excludeQueries = configuration.get(QUERY_ID);
        List<String> excludeTests = configuration.get(TEST_NAME);
        excludeQueries.add("char_128_2"); //failed query
        excludeQueries.add("map_test_varchar_02"); //fix PT code assertion (unsupported type MAP)
        excludeQueries.add("map_test_varchar_03"); //fix PT code assertion (unsupported type MAP)
        excludeQueries.add("map_test_varchar_04"); //fix PT code assertion (unsupported type MAP)
        excludeTests.add("row_test"); //fix PT code assertion
        excludeTests.add("array_with_nulls"); //fix PT code assertion
        excludeTests.add("nested_rows"); //test index put of bound, unsupported type
        excludeTests.add("varbinary_test"); //unsupported type
        ExcludeStrategy excludeStrategy = new ExcludeStrategy();
        List<TestFormat> parsedTests = excludeStrategy.parse(configuration, tests);
        return parsedTests.stream()
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS, dataProvider = "cache")
    public void cache(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, "synthetic");
    }

    @DataProvider
    public Iterator<Object[]> synthetic(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json");
    }

    @DataProvider
    public Iterator<Object[]> syntheticWithoutAggregations(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synthetic.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        String excludeString = "select count(*) from";
        HashMap<String, List<String>> configuration = excludeQueriesByPattern(excludeString, tests);
        List<String> excludeTests = configuration.get(TEST_NAME);
        List<String> excludeQueries = configuration.get(QUERY_ID);

        // These queries fail on "appendVarlenBlock found a string length %d longer than max %d" - so it's OK to skip them
        // It doesn't happen without caching manager because caching manager doesn't support dictionary
        excludeQueries.add("wide_40k_3");
        excludeQueries.add("varchar_rectlength_1");

        // Sometimes this query get stuck because of a deadlock that should be resolved once we move to the asynchronous approach
        // (see https://starburstdata.atlassian.net/browse/SIC-2178?focusedCommentId=150236).
        // Please note that we also used to get a PANIC for this query, but it doesn't seem to reproduce anymore (at least not on a local docker).
        // PANIC chunk start_loc 343 or type 3 or nv 125 are invalid. read_min_offset 220 read_max_offset 2317 chunk_ix 0 nchunks 16 relative_start_loc 1973 chunks_map_start_loc 2316
        // (Note: happened also before multi-column, see https://github.com/starburstdata/varada/actions/runs/8680131130/job/23800199602?pr=2762).
        excludeQueries.add("tuples_40keys_6");

        // This PR https://github.com/starburstdata/cork/pull/713 should have fixed the following query - need to validate on CI.
        // For now, we keep skipping it because the warmup is too long.
        // Before the PR we used to get an exception: WarpCacheColumnHandle cannot be cast to class io.trino.plugin.hive.HiveColumnHandle
        excludeTests.add("denorm_table");

        ExcludeStrategy excludeStrategy = new ExcludeStrategy();
        configuration.put(QUERY_ID, excludeQueries);
        configuration.put(TEST_NAME, excludeTests);
        List<TestFormat> parsedTests = excludeStrategy.parse(configuration, tests);
        return parsedTests.stream()
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS, dataProvider = "syntheticWithoutAggregations")
    public void syntheticWithoutAggregations(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        execute(testFormat, "synthetic");
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, "synthetic");
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS, dataProvider = "synthTypes")
    public void synthTypes(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        execute(testFormat, "synthetic");
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS)
    public void testPanicOnWrite()
            throws IOException
    {
        try {
            onTrino().executeQuery("USE warp.synthetic");
            onTrino().executeQuery("set session cache_aggregations_enabled = false");
            String query = "select id from mac_paramsj where id > 1";
            demoterUtils.resetToDefaultDemoterConfiguration();
            List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorData = List.of(new FailureGeneratorResource.FailureGeneratorData(
                    null,
                    "2388",
                    FailureRepetitionMode.REP_MODE_ONCE,
                    FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                    0));
            onTrino().executeQuery("set session warp.enable_default_warming = false");

            //now test failure
            restUtils.executeWorkerRestCommand(
                    FailureGeneratorResource.TASK_NAME,
                    "",
                    failureGeneratorData,
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_NO_CONTENT);

            //first query fails due to storage exception
            onTrino().executeQuery(query);

            for (int i = 0; i < 2; i++) {
                //query mark as failed so it will not read from cache
                QueryResult queryResult = onTrino().executeQuery(query);
                String queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
                ruleUtils.validateNotLoadByCacheDataOperator(queryId);
            }

            String query2 = "select id from mac_paramsj where id > 2";
            QueryResult queryResult = onTrino().executeQuery(query2);
            String queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
            ruleUtils.validateNotLoadByCacheDataOperator(queryId);

            //now it is warm and read from cache
            queryResult = onTrino().executeQuery(query2);
            assertThat(queryResult.getRowsCount()).isEqualTo(2);
            queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
            ruleUtils.validateLoadByCacheDataOperator(queryId);
        }
        catch (Exception e) {
            logger.error(e, "failed on testPanicOnWrite");
            throw e;
        }
        finally {
            demoterUtils.demoteAllByMaxUsage();
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
    }

    private void execute(TestFormat testFormat, String schemaName)
            throws IOException
    {
        if (testFormat.skip() || testFormat.skip_caching()) {
            logger.info("test %s is skipped. description=%s", testFormat.name(), testFormat.description());
            throw new SkipException("Skipping this test");
        }
        demoterUtils.resetToDefaultDemoterConfiguration();
        int ranQueries = 0;
        try {
            logger.info("starting run test %s", testFormat.name());
            onTrino().executeQuery("set session warp.enable_default_warming = false");
            onTrino().executeQuery(format("set session warp.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'", formattedDateTime));
            onTrino().executeQuery("set session warp.enable_import_export = true");
            onTrino().executeQuery(format("USE warp.%s", schemaName));
            ranQueries = queryUtils.runCacheQueries(testFormat);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw e;
        }
        finally {
            if (ranQueries > 0) {
                logger.info("run demoter after running %s queries on test %s", ranQueries, testFormat.name());
                demoterUtils.demoteAllByMaxUsage();
                demoterUtils.resetToDefaultDemoterConfiguration();
            }
        }
    }

    private HashMap<String, List<String>> excludeQueriesByPattern(String pattern, List<TestFormat> tests)
    {
        List<String> excludeQueries = new ArrayList<>();
        List<String> excludeTests = new ArrayList<>();
        for (TestFormat test : tests) {
            if (test.queries_data() == null) {
                excludeTests.add(test.name());
                continue;
            }
            for (TestFormat.QueryData query : test.queries_data()) {
                if (query.query().toLowerCase(Locale.ROOT).contains(pattern) || query.skip_caching()) {
                    excludeQueries.add(query.query_id());
                    logger.info("exclude query %s, id=%s", query.query(), query.query_id());
                }
            }
        }
        HashMap<String, List<String>> configuration = new HashMap<>();
        configuration.put(QUERY_ID, excludeQueries);
        configuration.put(TEST_NAME, excludeTests);
        return configuration;
    }
}
