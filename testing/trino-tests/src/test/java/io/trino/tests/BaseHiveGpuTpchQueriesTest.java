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
package io.trino.tests;

import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.IntStream;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.tests.GpuQueriesTests.assertGpuQueryResultsAndOperators;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class BaseHiveGpuTpchQueriesTest
        extends GpuQueriesTests.GpuPlanTest
{
    private static final Logger log = Logger.get(BaseHiveGpuTpchQueriesTest.class);

    static final int SCALE_FACTOR = 1;

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                // HiveQueryRunner.populateData() is hardcoded to copy from tpch.tiny; bypass it and create the sf1 tables ourselves.
                .setCreateTpchSchemas(false)
                // DECIMAL avoids floating-point sum-order instability that makes Q15's `revenue = MAX(revenue)` predicate flaky on the default DOUBLE mapping.
                .setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL)
                .setSkipTimezoneSetup(true)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .addHiveProperty("hive.parquet.time-zone", "UTC");
        configureRunner(builder);
        QueryRunner runner = builder.build();
        try {
            runner.execute("CREATE SCHEMA hive.tpch");
            for (TpchTable<?> table : TpchTable.getTables()) {
                String name = table.getTableName();
                long start = System.nanoTime();
                runner.execute("CREATE TABLE hive.tpch.%s WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf%d.%s"
                        .formatted(name, SCALE_FACTOR, name));
                log.info("Loaded hive.tpch.%s from tpch.sf%d in %d ms", name, SCALE_FACTOR, (System.nanoTime() - start) / 1_000_000);
            }
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    protected abstract void configureRunner(HiveQueryRunner.Builder<?> builder);

    @ParameterizedTest(name = "q{0}")
    @MethodSource("queries")
    public final void testQuery(int queryNumber)
            throws Exception
    {
        assertGpuQueryResultsAndOperators(
                getQueryRunner(),
                readQuery(queryNumber),
                readExpectedGpuPlanCoverage(queryNumber));
    }

    @Override
    final IntStream queries()
    {
        return IntStream.rangeClosed(1, 22);
    }

    @Override
    final String readQuery(int queryNumber)
            throws IOException
    {
        return Resources.toString(getResource("sql/trino/tpch/q%02d.sql".formatted(queryNumber)), UTF_8)
                .replace("${database}", "hive")
                .replace("${schema}", "tpch")
                .replace("${prefix}", "")
                .replace("${scale}", String.valueOf(SCALE_FACTOR))
                .trim()
                .replaceFirst(";$", "");
    }

    private String readExpectedGpuPlanCoverage(int queryNumber)
            throws IOException
    {
        return Resources.toString(getResource(gpuPlanResource(queryNumber)), UTF_8);
    }
}
