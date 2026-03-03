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

package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.inject.Inject;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.warp.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.tempto.query.QueryResult;
import jakarta.ws.rs.HttpMethod;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getDiffFromInitial;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getValue;
import static io.trino.tests.product.warp.utils.WarmUtils.verifyNoWarmups;
import static org.assertj.core.api.Assertions.assertThat;

public class DemoterUtils
{
    @Inject
    RestUtils restUtils;

    private static final Logger logger = Logger.get(DemoterUtils.class);

    private static final int LARGE_DEMOTER_BATCH_SIZE = 1000;
    private static final int DEFAULT_DEMOTER_BATCH_SIZE = 50;
    private static final int DEFAULT_DEMOTER_MAX_USAGE_THRESHOLD = 90;
    private static final int DEFAULT_DEMOTER_CLEANUP_THRESHOLD = 70;

    private static final double DEMOTE_CLEAN_UP_USAGE = 0;
    public static final JsonMapper jsonMapper = new JsonMapperProvider().get();

    private static final List<WarmUpType> allWarmUpTypes = List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC, WARM_UP_TYPE_LUCENE);

    public void demote(int port, String schema, String table, TestFormat test)
    {
        demote(port, schema, table, test.structure().stream().map(TestFormat.Column::name).toList());
    }

    public void demote(int port, String schema, String table, List<String> columnNames)
    {
        demote(port,
                schema,
                table,
                columnNames,
                -0.99,
                DEMOTE_CLEAN_UP_USAGE,
                true,
                true,
                false);
    }

    public void demote(
            int port,
            String schema,
            String table,
            List<String> columnNames,
            double cleanupUsageThresholdInPercentage,
            double maxUsageThresholdInPercentage,
            boolean modifyConfig,
            boolean executeDemoter,
            boolean longDemote)
    {
        int batchSize = longDemote ? LARGE_DEMOTER_BATCH_SIZE : DEFAULT_DEMOTER_BATCH_SIZE;
        WarmupDemoterData.Builder warmupDemoterDataBuilder = WarmupDemoterData.builder()
                .executeDemoter(executeDemoter)
                .batchSize(batchSize)
                .maxUsageThresholdInPercentage(maxUsageThresholdInPercentage)
                .modifyConfig(modifyConfig)
                .cleanupUsageThresholdInPercentage(cleanupUsageThresholdInPercentage)
                .resetHighestPriority(true)
                .forceExecuteDeadObjects(true)
                .forceDeleteFailedObjects(false)
                .defaultRuleTtlInSeconds(1200);

        if (schema != null && table != null) {
            warmupDemoterDataBuilder.schemaTableName(new SchemaTableName(schema, table));
            if (columnNames != null && !columnNames.isEmpty()) {
                warmupDemoterDataBuilder.warmupElementsData(
                        columnNames.stream()
                                .map(columnName -> new WarmupDemoterWarmupElementData(columnName, allWarmUpTypes))
                                .toList());
            }
        }
        demote(warmupDemoterDataBuilder.build(), port, false);
    }

    public Map<String, Object> demote(WarmupDemoterData warmupDemoterData, int port, boolean isCache)
    {
        Map<String, Object> res = new HashMap<>();
        try {
            verifyNoWarmups();
            verifyNoImports();
            QueryResult demoterStatsBefore = JMXCachingManager.getDemoterStats();
            logger.info("execute demote: SchemaTableName=%s", warmupDemoterData.getSchemaTableName());
            logger.debug("demote: warmupDemoterData %s", warmupDemoterData);
            String result;
            if (isCache) {
                result = restUtils.executeCacheRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH, WorkerWarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
            }
            else {
                result = restUtils.executePostCommandWithReturnValue(port, WarmupDemoterTask.WARMUP_DEMOTER_PATH, WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData);
            }
            res = jsonMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(result);
            logger.debug("%s", res);
            if (warmupDemoterData.isExecuteDemoter()) {
                QueryResult demoterStatsAfter = JMXCachingManager.getDemoterStats();
                assertDemoteCompleted(demoterStatsAfter, demoterStatsBefore);
            }
        }
        catch (Exception e) {
            logger.error(e, "demoter failed %s", warmupDemoterData);
        }
        return res;
    }

    public void assertDemoteCompleted(QueryResult demoterStatsAfter, QueryResult demoterStatsBefore)
            throws Exception
    {
        if (!(getDiffFromInitial(demoterStatsAfter, demoterStatsBefore, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS) > 0 ||
                getDiffFromInitial(demoterStatsAfter, demoterStatsBefore, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS_FAIL) > 0)) {
            throw new Exception(String.format("Demoter did not run NUMBER OF RUNS: %d should be > %d or NUMBER_OF_RUNS_FAIL: %d should be > %d",
                    getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS),
                    getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS),
                    getValue(demoterStatsAfter, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS_FAIL),
                    getValue(demoterStatsBefore, JMXCachingConstants.WarmupDemoter.NUMBER_OF_RUNS_FAIL)));
        }
    }

    private void verifyNoImports()
    {
        assertEventually(
                Duration.valueOf("20s"),
                () -> {
                    QueryResult importStats = JMXCachingManager.getImportStats();
                    assertThat(getValue(importStats, JMXCachingConstants.WarmupImportService.IMPORT_ROW_GROUP_COUNT_STARTED))
                            .isEqualTo(getValue(importStats, JMXCachingConstants.WarmupImportService.IMPORT_ROW_GROUP_COUNT_ACCOMPLISHED));
                });
    }

    public void resetToDefaultDemoterConfiguration(int port)
            throws IOException
    {
        resetToDefaultDemoterConfiguration(port, false);
    }

    private void resetToDefaultDemoterConfiguration(int port, boolean isCache)
            throws IOException
    {
        WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                .executeDemoter(false)
                .batchSize(DEFAULT_DEMOTER_BATCH_SIZE)
                .maxUsageThresholdInPercentage(DEFAULT_DEMOTER_MAX_USAGE_THRESHOLD)
                .modifyConfig(true)
                .cleanupUsageThresholdInPercentage(DEFAULT_DEMOTER_CLEANUP_THRESHOLD)
                .resetHighestPriority(true)
                .forceExecuteDeadObjects(false)
                .forceDeleteFailedObjects(false)
                .build();
        logger.info("resetToDefaultDemoterConfiguration");
        String result;
        if (isCache) {
            result = restUtils.executeCacheRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH, WorkerWarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        }
        else {
            result = restUtils.executePostCommandWithReturnValue(port, WarmupDemoterTask.WARMUP_DEMOTER_PATH, WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData);
        }
        logger.debug("result demoter configuration: %s", result);
    }
}
