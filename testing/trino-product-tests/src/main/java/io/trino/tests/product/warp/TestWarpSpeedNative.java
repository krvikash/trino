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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource;
import io.trino.plugin.warp.extension.execution.debugtools.NativeStorageStateResource;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.plugin.warp.util.FailureGeneratorInvocationHandler;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.WarmUtils;
import jakarta.ws.rs.HttpMethod;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_MINIO;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.EXTERNAL_COLLECT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.EXTERNAL_MATCH;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.WARP_COLLECT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Columns.WARP_MATCH;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.ROW_GROUP_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED;
import static java.lang.String.format;

public class TestWarpSpeedNative
{
    private static final Logger logger = Logger.get(TestWarpSpeedNative.class);
    private static final String QUERY = "SELECT %s FROM %s WHERE nationkey > 1";
    private static final String BUCKET_NAME = "product-tests-warp-speed";
    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "warp_product_tests";

    @Inject
    DemoterUtils demoterUtils;
    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RestUtils restUtils;

    private final String tableName = "nation_native";
    private MinioClient client;

    @BeforeMethodWithContext
    public void beforeMethod()
    {
        client = new MinioClient();
        client.ensureBucketExists(BUCKET_NAME);

        try {
            restUtils.executeWorkerRestCommand(
                    NativeStorageStateResource.PATH,
                    "",
                    new NativeStorageStateResource.NativeStorageState(0, 0, false, false),
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_NO_CONTENT);

            QueryExecutor queryExecutor = onTrino();
            queryExecutor.executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s.%s WITH (location = 's3://%s/')", CATALOG_NAME, SCHEMA_NAME, BUCKET_NAME));
            queryExecutor.executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));
            queryExecutor.executeQuery("set session warp.enable_import_export = false");
            queryExecutor.executeQuery(format("CREATE TABLE IF NOT EXISTS %s.%s.%s AS SELECT * FROM tpch.tiny.nation", CATALOG_NAME, SCHEMA_NAME, tableName));
        }
        catch (Throwable e) {
            logger.error(e, "beforeMethod failed");
        }
    }

    @AfterMethodWithContext
    public void afterMethod()
    {
        try {
            restUtils.executeWorkerRestCommand(
                    NativeStorageStateResource.PATH,
                    "",
                    new NativeStorageStateResource.NativeStorageState(0, 0, false, false),
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_NO_CONTENT);

            demoterUtils.demote(
                    SCHEMA_NAME,
                    tableName,
                    List.of("name", "nationkey", "regionkey", "comment"),
                    -0.99,
                    0,
                    true,
                    true,
                    false);
            demoterUtils.resetToDefaultDemoterConfiguration();

            if (client != null) {
                client.close();
                client = null;
            }
        }
        catch (Throwable e) {
            logger.error(e, "afterMethod failed");
        }
    }

    @Test(groups = {WARP_SPEED_MINIO, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageWrite(ITestContext iTestContext)
            throws IOException
    {
        testWrite(List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2388", // generates panic id 2388 in function file_write_pages
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                iTestContext.getName());
    }

    @Test(groups = {WARP_SPEED_MINIO, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageRead(ITestContext iTestContext)
            throws IOException
    {
        testRead(List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2389", // generates panic id 2389 in function storage_internal_read_cache
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                iTestContext.getName());
    }

    @Test(groups = {WARP_SPEED_MINIO, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageWait(ITestContext iTestContext)
            throws IOException
    {
        testRead(List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2390", // generates panic id 2390 in function storage_wait
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                iTestContext.getName());
    }

    private void testRead(
            List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorDataList,
            String testName)
            throws IOException
    {
        logger.info("testRead::before warmAndValidate");
        //check that it works before setting failures
        warmUtils.warmAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(
                        WARM_ACCOMPLISHED, 1L,
                        ROW_GROUP_COUNT, 1L),
                Duration.valueOf("30s"));

        logger.info("testRead::before queryAndValidate");
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 2L,
                        WARP_MATCH, 1L,
                        EXTERNAL_COLLECT, 0L,
                        EXTERNAL_MATCH, 0L),
                testName);

        restUtils.validateNativeState(false, false);

        //now test failure
        restUtils.executeWorkerRestCommand(
                FailureGeneratorResource.TASK_NAME,
                "",
                failureGeneratorDataList,
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);

        logger.info("testRead::before assertQueryFailure");
        //first query fails due to storage exception
        assertQueryFailure(() -> onTrino().executeQuery(QUERY.formatted("name", tableName)))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("native storage engine");

        logger.info("testRead::before proxy queryAndValidate");
        //this one is served from proxy
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 0L,
                        WARP_MATCH, 0L,
                        EXTERNAL_COLLECT, 2L,
                        EXTERNAL_MATCH, 1L),
                testName);
        restUtils.validateNativeState(false, true);

        // reset storage state
        restUtils.executeWorkerRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(0, 0, false, false),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);
        logger.info("testRead::before additional queryAndValidate");

        //now we succeed
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 2L,
                        WARP_MATCH, 1L,
                        EXTERNAL_COLLECT, 0L,
                        EXTERNAL_MATCH, 0L),
                testName);
        restUtils.validateNativeState(false, false);
    }

    private void testWrite(
            List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorDataList,
            String testName)
            throws IOException
    {
        //now test failure
        restUtils.executeWorkerRestCommand(
                FailureGeneratorResource.TASK_NAME,
                "",
                failureGeneratorDataList,
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);

        logger.info("testWrite::before proxy queryAndValidate");
        //this one is served from proxy
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 0L,
                        WARP_MATCH, 0L,
                        EXTERNAL_COLLECT, 2L,
                        EXTERNAL_MATCH, 1L),
                testName);

        logger.info("testWrite::before second queryAndValidate");
        //since warming failed, this one is served from proxy as well
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 0L,
                        WARP_MATCH, 0L,
                        EXTERNAL_COLLECT, 2L,
                        EXTERNAL_MATCH, 1L),
                testName);

        // reset storage state
        restUtils.executeWorkerRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(0, 0, false, false),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);

        logger.info("testWrite::before third queryAndValidate");
        //now we succeed since no more storage exceptions
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(WARP_COLLECT, 2L,
                        WARP_MATCH, 1L,
                        EXTERNAL_COLLECT, 0L,
                        EXTERNAL_MATCH, 0L),
                testName);
    }
}
