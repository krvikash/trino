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

import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

public class TestHiveNoExtensionsProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestHiveNoExtensionsProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "hive_no_ext", false);
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
                        "node.environment", "varada",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME,
                        WarpExtensionConfig.ENABLED, "false"),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
    }

    @Test
    public void testBucketedBy()
    {
        String table = "test_bucketed_by";
        createTable(DEFAULT_SCHEMA,
                table,
                "(int_1 integer, bigint_2 bigint, smallint_3 smallint, tinyint_4 tinyint, char_5 char(9))" +
                        " WITH (format = 'PARQUET', bucketed_by = ARRAY['int_1'], bucket_count = 4)");

        IntStream.range(1, 4)
                .forEach(value -> assertUpdate("INSERT INTO %s (int_1, bigint_2, smallint_3, tinyint_4, char_5) VALUES (%d, %d, %d, %d, '%09d')"
                        .formatted(table, value, value, value, value, value), 1));

        @Language("SQL") String query = "SELECT * FROM " + table;
        warmAndValidate(query, true, 15, 3);

        validateQueryStats(
                query,
                getSession(),
                Map.of(
                        CACHED_TOTAL_ROWS, 3L,
                        WARP_MATCH_COLUMNS_STAT, 0L,
                        WARP_COLLECT_COLUMNS_STAT, 15L,
                        EXTERNAL_MATCH_STAT, 0L,
                        EXTERNAL_COLLECT_STAT, 0L));

        validateQueryStats(
                "SELECT * FROM %s WHERE int_1 = 1".formatted(table),
                getSession(),
                Map.of(
                        CACHED_TOTAL_ROWS, 1L,
                        WARP_MATCH_COLUMNS_STAT, 0L,
                        WARP_COLLECT_COLUMNS_STAT, 5L,
                        PREFILLED_COLUMNS_STAT, 0L,
                        EXTERNAL_MATCH_STAT, 1L,
                        EXTERNAL_COLLECT_STAT, 0L));
    }

    @Test
    public void testPartitionedBy()
    {
        String table = "test_partitioned_by";
        createTable(DEFAULT_SCHEMA,
                table,
                "(bigint_2 bigint, smallint_3 smallint, tinyint_4 tinyint, char_5 char(9), int_1 integer)" +
                        " WITH (format = 'PARQUET', partitioned_by = ARRAY['int_1'])");

        IntStream.range(1, 4)
                .forEach(value -> assertUpdate("INSERT INTO %s (bigint_2, smallint_3, tinyint_4, char_5, int_1) VALUES (%d, %d, %d, '%09d', %d)"
                        .formatted(table, value, value, value, value, value), 1));

        @Language("SQL") String query = "SELECT * FROM " + table;
        warmAndValidate(query, true, 15, 3);

        validateQueryStats(
                query,
                getSession(),
                Map.of(
                        CACHED_TOTAL_ROWS, 3L,
                        WARP_MATCH_COLUMNS_STAT, 0L,
                        WARP_COLLECT_COLUMNS_STAT, 12L,
                        EXTERNAL_MATCH_STAT, 0L,
                        EXTERNAL_COLLECT_STAT, 0L));

        validateQueryStats(
                "SELECT * FROM %s WHERE int_1 = 1".formatted(table),
                getSession(),
                Map.of(
                        CACHED_TOTAL_ROWS, 1L,
                        WARP_MATCH_COLUMNS_STAT, 0L,
                        WARP_COLLECT_COLUMNS_STAT, 4L,
                        PREFILLED_COLUMNS_STAT, 1L,
                        EXTERNAL_MATCH_STAT, 0L,
                        EXTERNAL_COLLECT_STAT, 0L));
    }
}
