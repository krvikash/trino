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

import io.trino.Session;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupPropertiesData;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterThreshold;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDemoterHiveProxiedConnectorIntegrationIT
        extends DispatcherStubsIntegrationSmokeIT
{
    private static final String WIDE_TABLE_NAME = "wide";

    public TestDemoterHiveProxiedConnectorIntegrationIT()
    {
        super(1, "hive_demote");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        "hive.s3.aws-access-key", "this is a fake key",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
    }

    @Test
    public void testWarmupDemoteAutomatic()
            throws IOException
    {
        @Language("SQL") String statQuery =
                """
                        select sum(number_of_runs),
                        sum(deleted_by_low_priority),
                        sum(number_of_calls),
                        sum(number_of_runs_fail)
                        from "%s*%s"
                        """
                        .formatted(
                                WarmupDemoterStats.class.getPackageName(),
                                WarmupDemoterStats.class.getSimpleName().toLowerCase(Locale.ROOT));
        Session jmxSession = createJmxSession();

        MaterializedRow materializedRow0 = computeActual(jmxSession, statQuery)
                .getMaterializedRows()
                .getFirst();
        long numOfRuns0 = (long) materializedRow0.getField(0);
        long numberOfDeletedByLowPrio0 = (long) materializedRow0.getField(1);
        buildAndWarmWideTable(10, false, 30, Optional.empty());

        demote(WarmupDemoterData.builder()
                .batchSize(2)
                .executeDemoter(false)
                .modifyConfig(true)
                .warmupDemoterThreshold(new WarmupDemoterThreshold(0.95, 0.6))
                .build());

        MaterializedRow materializedRow1 = computeActual(jmxSession, statQuery)
                .getMaterializedRows()
                .getFirst();
        long numOfRuns1 = (long) materializedRow1.getField(0);
        long numberOfDeletedByLowPrio1 = (long) materializedRow1.getField(1);
        long numberOfCalls1 = (long) materializedRow1.getField(2);
        long numberOfFails1 = (long) materializedRow1.getField(3);
        assertThat(numOfRuns0).isEqualTo(numOfRuns1);
        assertThat(numberOfDeletedByLowPrio0).isEqualTo(numberOfDeletedByLowPrio1);

        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        warmAndValidateLazyDemote("select * from t", true);

        MaterializedRow materializedRow2 = computeActual(jmxSession, statQuery)
                .getMaterializedRows()
                .getFirst();
        long numOfRuns2 = (long) materializedRow2.getField(0);
        long numberOfCalls2 = (long) materializedRow2.getField(2);
        long numberOfDeletedByLowPrio2 = (long) materializedRow2.getField(1);
        long numberOfFails2 = (long) materializedRow2.getField(3);
        assertThat(numberOfFails2)
                .describedAs("numberOfFails")
                .isEqualTo(numberOfFails1);
        assertThat(numberOfDeletedByLowPrio2)
                .describedAs("numberOfDeletedByLowPrio")
                .isGreaterThan(numberOfDeletedByLowPrio1);
        assertThat(numberOfCalls2)
                .describedAs("numberOfCalls")
                .isGreaterThan(numberOfCalls1);
        assertThat(numOfRuns2)
                .describedAs("numOfRuns")
                .isEqualTo(numOfRuns1 + 1);
    }

    @Test
    public void testSimpleWarmupSyncDemoter()
            throws IOException
    {
        buildAndWarmWideTable(3, false, 9, Optional.of(Duration.ofMinutes(0)));
        Map<String, Object> res = demote(WarmupDemoterData.builder()
                .maxUsageThresholdInPercentage(31d)
                .cleanupUsageThresholdInPercentage(21d)
                .batchSize(10)
                .executeDemoter(true)
                .forceExecuteDeadObjects(true)
                .forceDeleteFailedObjects(true)
                .modifyConfig(true)
                .build());

        Integer deadObjectsDeletedCount = (Integer) res.entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith("dead_objects_deleted"))
                .findAny()
                .orElseThrow()
                .getValue();
        Integer deletedByLowPriorityCount = (Integer) res.entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith("deleted_by_low_priority"))
                .findAny()
                .orElseThrow()
                .getValue();
        assertThat(deadObjectsDeletedCount).isEqualTo(9);
        assertThat(deletedByLowPriorityCount).isEqualTo(0);
    }

    @Test
    public void testWarmupDemoterWithFilterShouldDemoteOnlyByTable()
            throws IOException
    {
        int numberOfColumns = 3;
        int expectedElementCount = 9;
        buildAndWarmWideTable(numberOfColumns, false, expectedElementCount, Optional.empty());

        List<WarmupDemoterWarmupElementData> warmupDemoterWarmupElementDataList = new ArrayList<>(expectedElementCount);
        IntStream.range(0, numberOfColumns).forEach(columnId -> {
            warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c0" + columnId, Collections.emptyList()));
            warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c1" + columnId, Collections.emptyList()));
            warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c2" + columnId, Collections.emptyList()));
        });

        Map<String, Object> res = demote(WarmupDemoterData.builder()
                .maxUsageThresholdInPercentage(31d)
                .cleanupUsageThresholdInPercentage(21d)
                .batchSize(10)
                .executeDemoter(true)
                .forceExecuteDeadObjects(true)
                .schemaTableName(new SchemaTableName(DEFAULT_SCHEMA, WIDE_TABLE_NAME))
                .warmupElementsData(warmupDemoterWarmupElementDataList)
                .resetHighestPriority(true)
                .build());

        Integer deadObjectsDeletedCount = (Integer) res.entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith("dead_objects_deleted"))
                .findAny()
                .orElseThrow()
                .getValue();
        Integer deletedByLowPriorityCount = (Integer) res.entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith("deleted_by_low_priority"))
                .findAny()
                .orElseThrow()
                .getValue();
        Double highestPriority = (Double) res.entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.HIGHEST_PRIORITY_KEY))
                .findAny()
                .orElseThrow()
                .getValue();
        assertThat(highestPriority).isEqualTo(0);
        assertThat(deadObjectsDeletedCount).isEqualTo(9);
        assertThat(deletedByLowPriorityCount).isEqualTo(0);
    }

    private void buildAndWarmWideTable(
            int numberOfColumns,
            boolean defaultWarm,
            int expectedElementsToWarm,
            Optional<Duration> duration)
            throws IOException
    {
        StringJoiner columnDefinition = new StringJoiner(",", "(", ")");
        StringJoiner values = new StringJoiner(",");
        Map<String, Set<WarmupPropertiesData>> rules = new HashMap<>();
        IntStream.range(0, numberOfColumns).forEach(columnId -> {
            columnDefinition.add("C0" + columnId + " varchar(20)");
            values.add("'value0" + columnId + "'");
            if (!defaultWarm) {
                rules.put("C0" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 7.5, duration.orElse(Duration.ofMinutes(columnId * 10L)))));
            }

            columnDefinition.add("C1" + columnId + " integer");
            values.add(String.valueOf(columnId));
            if (!defaultWarm) {
                rules.put("C1" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 7.1, duration.orElse(Duration.ofMinutes(columnId)))));
            }

            columnDefinition.add("C2" + columnId + " tinyint");
            values.add("1");
            if (!defaultWarm) {
                rules.put("C2" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 6.3, duration.orElse(Duration.ofSeconds(columnId * 10L)))));
            }
        });

        createTable(DEFAULT_SCHEMA, WIDE_TABLE_NAME, columnDefinition.toString());
        if (!defaultWarm) {
            createWarmupRules(DEFAULT_SCHEMA, WIDE_TABLE_NAME, rules);
        }
        computeActual(getSession(), format("INSERT INTO %s VALUES (%s)", WIDE_TABLE_NAME, values));
        Session session = buildSession(defaultWarm, false);
        int expectedWarmAccomplished = expectedElementsToWarm == 0 ? 0 : 1;
        warmAndValidate(format("select * from %s", WIDE_TABLE_NAME), session, expectedElementsToWarm, expectedWarmAccomplished, 0);
    }
}
