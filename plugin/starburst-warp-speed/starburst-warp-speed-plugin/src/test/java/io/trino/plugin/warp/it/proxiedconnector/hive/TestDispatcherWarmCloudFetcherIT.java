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
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.api.warmup.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.api.warmup.column.RegularColumnData;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfig;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.it.DispatcherAbstractTestQueryFramework;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.tools.util.CompressionUtil;
import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDispatcherWarmCloudFetcherIT
        extends DispatcherAbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "cloud_fetcher";

    private Path warmRulesStorePath;

    @AfterEach
    public void afterTest()
    {
        cleanModel();
    }

    @BeforeEach
    @Override
    public void beforeMethod(TestInfo testInfo)
    {
        super.beforeMethod(testInfo);
        initializeWorkers(CATALOG_NAME);
    }

    private void cleanModel()
    {
        createdTables.forEach(tableName -> assertUpdate("DROP TABLE IF EXISTS " + tableName));
        createdSchemas.forEach(schemaName -> assertUpdate("DROP SCHEMA IF EXISTS " + schemaName));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warmRulesStorePath = Files.createTempDirectory("store_path");

        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                2,
                Map.of(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME,
                        WarmupRuleCloudFetcherConfig.STORE_PATH, warmRulesStorePath.toFile().getAbsolutePath(),
                        WarmupRuleCloudFetcherConfig.STORE_TYPE, StoreType.LOCAL.name()),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG_NAME,
                new WarpPlugin(),
                Collections.emptyMap());
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());
        return queryRunner;
    }

    @Test
    public void testWarmupApi()
            throws IOException
    {
        assertThat(getWarmupRules()).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        executeRestCommand(
                WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_SET,
                getRules(WarmUpType.WARM_UP_TYPE_BASIC),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK);

        assertThat(getWarmupRules()).isEmpty();

        createWarmRulesFileForCloudFetcher(WarmUpType.WARM_UP_TYPE_DATA);

        String restStrResult = executeRestCommand(
                WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_FETCH,
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        Map<String, List<WarmupColRuleData>> workerWarmupColRuleDatasMap = jsonMapper.readerFor(new TypeReference<Map<String, List<WarmupColRuleData>>>() {})
                .readValue(restStrResult);
        assertThat(workerWarmupColRuleDatasMap).hasSize(1);
        workerWarmupColRuleDatasMap.forEach((_, value) -> {
            assertThat(value
                    .stream()
                    .map(warmupColRuleData -> new WarmupColRuleData(
                            0,
                            warmupColRuleData.getSchema(),
                            warmupColRuleData.getTable(),
                            warmupColRuleData.getColumn(),
                            warmupColRuleData.getWarmUpType(),
                            warmupColRuleData.getPriority(),
                            warmupColRuleData.getTtl(),
                            warmupColRuleData.getPredicates()))
                    .toList())
                    .isEqualTo(getRules(WarmUpType.WARM_UP_TYPE_DATA));
        });

        createWarmRulesFileForCloudFetcher(WarmUpType.WARM_UP_TYPE_LUCENE);

        restStrResult = executeRestCommand(
                WarmupRuleService.WARMUP_PATH,
                WarmupRuleService.TASK_NAME_GET,
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        List<WarmupColRuleData> warmupColRuleDataList = jsonMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(restStrResult);
        assertThat(warmupColRuleDataList.stream()
                .map(warmupColRuleData -> new WarmupColRuleData(
                        0,
                        warmupColRuleData.getSchema(),
                        warmupColRuleData.getTable(),
                        warmupColRuleData.getColumn(),
                        warmupColRuleData.getWarmUpType(),
                        warmupColRuleData.getPriority(),
                        warmupColRuleData.getTtl(),
                        warmupColRuleData.getPredicates()))
                .toList())
                .isEqualTo(getRules(WarmUpType.WARM_UP_TYPE_LUCENE));
    }

    private void createWarmRulesFileForCloudFetcher(WarmUpType... types)
            throws IOException
    {
        Path warmRulesFilePath = Path.of("%s/%s".formatted(warmRulesStorePath.toFile().getAbsolutePath(), CATALOG_NAME));
        FileUtils.createParentDirectories(warmRulesFilePath.toFile());

        if (warmRulesFilePath.toFile().exists()) {
            FileUtils.delete(warmRulesFilePath.toFile());
        }

        if (!warmRulesFilePath.toFile().createNewFile()) {
            throw new RuntimeException("failed creating file " + warmRulesFilePath);
        }
        Files.write(
                warmRulesFilePath,
                CompressionUtil.compressGzip(jsonMapper.writeValueAsString(getRules(types))));
    }

    private List<WarmupColRuleData> getRules(WarmUpType... types)
    {
        return Stream.of(types).map(type ->
                        new WarmupColRuleData(
                                0,
                                "s1",
                                "t1",
                                new RegularColumnData("col2"),
                                type,
                                5,
                                Duration.ofHours(10),
                                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", ""))))
                .toList();
    }
}
