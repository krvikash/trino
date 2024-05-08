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
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.syntheticconfig.ExcludeStrategy;
import io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration;
import org.testng.ITestContext;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_CACHE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.QUERY_ID;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.TEST_NAME;
import static java.lang.String.format;

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
        List<TestFormat> testsToExecute = new ArrayList<>(tests);
        if (TestConfiguration.hasRunConfiguration()) {
            if (TestConfiguration.hasTestFilter()) {
                testsToExecute = TestConfiguration.filterTests(tests);
            }
            testsToExecute = TestConfiguration.updateTableType(testsToExecute);
            logger.info("Suite run on table type %s", TestConfiguration.getTableType().name());
        }
        return testsToExecute
                .stream()
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

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "cache")
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
        excludeQueries.add("wide_40k_3");
        excludeQueries.add("varchar_rectlength_1");

        // We need to check why these queries fail
        excludeQueries.add("tuples_40keys_2"); // PANIC insufficient write buffer (on query time)
        excludeQueries.add("tuples_40keys_3"); // PANIC insufficient write buffer (on query time)
        excludeQueries.add("tuples_40keys_4"); // PANIC illegal number of compressed pages 0 to write (max 64)
        excludeQueries.add("tuples_40keys_5"); // PANIC caught sig 11, siginfo: signo 11 code 1 errno 0 \ page_ix 249 uncomp offset page 0 (on query time)
        excludeQueries.add("tuples_40keys_6"); // PANIC chunk start_loc 343 or type 3 or nv 125 are invalid. read_min_offset 220 read_max_offset 2317 chunk_ix 0 nchunks 16 relative_start_loc 1973 chunks_map_start_loc 2316 (Note: happened also before multi-column, see https://github.com/starburstdata/varada/actions/runs/8680131130/job/23800199602?pr=2762). Query is getting stuck when trying to run locally.
        excludeQueries.add("tuples_40keys_10"); // PANIC illegal number of compressed pages 0 to write (max 64)
        excludeQueries.add("tuples_40keys_11"); // PANIC illegal number of compressed pages 0 to write (max 64)

        // This PR https://github.com/starburstdata/cork/pull/713 will fix the following
        excludeTests.add("denorm_table"); // long warm + exception skip denorm_table on exception WarpCacheColumnHandle cannot be cast to class io.trino.plugin.hive.HiveColumnHandle

        ExcludeStrategy excludeStrategy = new ExcludeStrategy();
        configuration.put(QUERY_ID, excludeQueries);
        configuration.put(TEST_NAME, excludeTests);
        List<TestFormat> parsedTests = excludeStrategy.parse(configuration, tests);
        return parsedTests.stream()
                .map(x -> new Object[] {x})
                .iterator();
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "syntheticWithoutAggregations")
    public void syntheticWithoutAggregations(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        execute(testFormat, "synthetic");
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, "synthetic");
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthTypes")
    public void synthTypes(TestFormat testFormat)
            throws IOException
    {
        onTrino().executeQuery("set session cache_aggregations_enabled = false");
        execute(testFormat, "synthetic");
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
