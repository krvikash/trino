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
import io.trino.tests.product.warp.utils.CacheUtils;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.JMXCachingConstants;
import io.trino.tests.product.warp.utils.JMXCachingManager;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestCacheFormat;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.syntheticconfig.ExcludeStrategy;
import jakarta.ws.rs.HttpMethod;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_CACHE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getDiffFromInitial;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.QUERY_ID;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.TEST_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWarpCache
{
    private static final Logger logger = Logger.get(TestWarpCache.class);
    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "synthetic";

    private boolean initialized;

    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;
    @Inject
    RestUtils restUtils;
    @Inject
    CacheUtils cacheUtils;

    public TestWarpCache()
    {
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
                onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME, SCHEMA_NAME));
                onTrino().executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));
                initialized = true;
            }
        }
        ruleUtils.resetAllRules();
    }

    @AfterMethodWithContext
    public void after()
    {
    }

    @DataProvider
    public Iterator<Object[]> synthTypes(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synth_types.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        List<String> excludePatterns = List.of(
                "select count(*) from", // not supported yet
                "limit "); // limit queries get aborted
        HashMap<String, List<String>> configuration = excludeQueriesByPatterns(excludePatterns, tests);
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

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthTypes")
    public void synthTypes(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        cacheUtils.execute(testFormat, true, SCHEMA_NAME);
    }

    @DataProvider
    public Iterator<Object[]> storeId(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synthetic_cache_manager.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestCacheFormat> tests = objectMapper.readerFor(new TypeReference<List<TestCacheFormat>>() {})
                .readValue(jsonNodeTests);
        return tests.stream()
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "storeId")
    public void storeId(TestCacheFormat testFormat)
            throws IOException
    {
        try {
            onTrino().executeQuery("USE warp.synthetic");
            onTrino().executeQuery("set session warp.enable_default_warming=False");
            QueryResult warmingStatsBefore = JMXCachingManager.getWarmingStats();
            cacheUtils.runQueries(testFormat.queries_data(), false);
            cacheUtils.runQueries(testFormat.queries_data(), true);
            QueryResult warmingStatsAfter = JMXCachingManager.getWarmingStats();
            long rowGroupCount = getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, JMXCachingConstants.WarmingService.ROW_GROUP_COUNT);
            long warmupElementCount = getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, JMXCachingConstants.WarmingService.WARMUP_ELEMENTS_COUNT);
            assertThat(rowGroupCount).isEqualTo(testFormat.expected_row_group());
            assertThat(warmupElementCount).isEqualTo(testFormat.expected_warmup_elements());
        }
        finally {
            demoterUtils.demoteAllByMaxUsage(true);
            demoterUtils.resetToDefaultDemoterConfiguration(true);
        }
    }

    @DataProvider
    public Iterator<Object[]> synthetic(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synthetic.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        List<String> excludePatterns = List.of("limit "); // limit queries get aborted
        HashMap<String, List<String>> configuration = excludeQueriesByPatterns(excludePatterns, tests);
        List<String> excludeTests = configuration.get(TEST_NAME);
        List<String> excludeQueries = configuration.get(QUERY_ID);

        ExcludeStrategy excludeStrategy = new ExcludeStrategy();
        configuration.put(QUERY_ID, excludeQueries);
        configuration.put(TEST_NAME, excludeTests);
        List<TestFormat> parsedTests = excludeStrategy.parse(configuration, tests);
        return parsedTests.stream()
                .filter(TestFormat::pt_enable)
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        cacheUtils.execute(testFormat, true, SCHEMA_NAME);
    }

    @DataProvider
    public Iterator<Object[]> syntheticWithoutAggregations(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/presto-product-tests/warp/synthetic.json";
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        List<String> excludePatterns = List.of(
                "select count(*) from", // not supported yet
                "limit "); // limit queries get aborted
        HashMap<String, List<String>> configuration = excludeQueriesByPatterns(excludePatterns, tests);
        List<String> excludeTests = configuration.get(TEST_NAME);
        List<String> excludeQueries = configuration.get(QUERY_ID);

        // These queries fail on "appendVarlenBlock found a string length %d longer than max %d" - so it's OK to skip them
        // It doesn't happen without caching manager because caching manager doesn't support dictionary
        excludeQueries.add("wide_40k_3");
        excludeQueries.add("varchar_rectlength_1");
        ExcludeStrategy excludeStrategy = new ExcludeStrategy();
        configuration.put(QUERY_ID, excludeQueries);
        configuration.put(TEST_NAME, excludeTests);
        List<TestFormat> parsedTests = excludeStrategy.parse(configuration, tests);
        return parsedTests.stream()
                .filter(TestFormat::pt_enable)
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "syntheticWithoutAggregations")
    public void syntheticWithoutAggregations(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        cacheUtils.execute(testFormat, true, SCHEMA_NAME);
    }

    @Test(groups = PROFILE_SPECIFIC_TESTS)
    public void testPanicOnWrite()
            throws IOException
    {
        try {
            onTrino().executeQuery("USE warp.synthetic");
            onTrino().executeQuery("set session cache_aggregations_enabled = false");
            String query = "select id from mac_paramsj where id > 1";
            demoterUtils.resetToDefaultDemoterConfiguration(true);
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
            demoterUtils.demoteAllByMaxUsage(true);
            demoterUtils.resetToDefaultDemoterConfiguration(true);
        }
    }

    private HashMap<String, List<String>> excludeQueriesByPatterns(Collection<String> patterns, List<TestFormat> tests)
    {
        List<String> excludeQueries = new ArrayList<>();
        List<String> excludeTests = new ArrayList<>();
        for (TestFormat test : tests) {
            if (test.queries_data() == null) {
                excludeTests.add(test.name());
                continue;
            }
            for (TestFormat.QueryData query : test.queries_data()) {
                if (patterns.stream().anyMatch(pattern -> query.query().toLowerCase(Locale.ROOT).contains(pattern)) || query.skip_caching()) {
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
