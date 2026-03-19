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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.SystemSessionProperties.WRITER_SCALING_MIN_DATA_PROCESSED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Integration tests verifying that MERGE with writer scaling enabled produces correct results.
 * Writer scaling is only safe for MoR (merge-on-read) mode; for CoW (copy-on-write) it is
 * disabled by {@link IcebergMetadata#getMergeWriterScalingOptions}.
 *
 * <p>In CoW mode, a MERGE rewrites entire data files that contain any modified rows. When writer
 * scaling is enabled, the engine may add additional writer tasks mid-query. Each new writer
 * independently copies unmodified rows from the same source files, resulting in those rows
 * appearing multiple times in the output — i.e., duplicate rows. This is why
 * {@code getMergeWriterScalingOptions} returns {@code DISABLED} for CoW tables.
 *
 * <p>Uses {@code tpch.tiny} (1.5k rows) for speed. Note that the CoW duplication bug
 * originally only reproduced at sf10 scale (15M rows).
 */
@TestInstance(PER_CLASS)
public class TestIcebergMergeWriterScaling
        extends AbstractTestQueryFramework
{
    private long ordersCount;
    private String sourceTable;
    private long deletedCount;
    private long insertedCount;
    private long updatedCount;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setWorkerCount(3)
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        ordersCount = (long) computeScalar("SELECT COUNT(*) FROM tpch.tiny.orders");
        sourceTable = "test_merge_scaling_source_" + randomNameSuffix();
        updatedCount = (long) computeScalar("SELECT COUNT(*) FROM tpch.tiny.orders WHERE custkey % 3 = 0");
        deletedCount = (long) computeScalar("SELECT COUNT(*) FROM tpch.tiny.orders WHERE custkey % 3 = 1");
        insertedCount = (long) computeScalar("SELECT COUNT(*) FROM tpch.tiny.orders WHERE custkey % 5 = 0 AND orderkey % 10 = 0");
        long sourceRowCount = updatedCount + deletedCount + insertedCount;
        // Source: mix of rows for UPDATE (custkey%3=0), DELETE (custkey%3=1), INSERT (negative orderkey)
        assertUpdate(format("""
                CREATE TABLE %s AS
                SELECT orderkey, custkey, orderstatus, totalprice, orderpriority, clerk, 'updated_' || clerk AS new_clerk
                FROM tpch.tiny.orders WHERE custkey %% 3 = 0
                UNION ALL
                SELECT orderkey, custkey, orderstatus, totalprice, orderpriority, clerk, clerk AS new_clerk
                FROM tpch.tiny.orders WHERE custkey %% 3 = 1
                UNION ALL
                SELECT -(orderkey) AS orderkey, custkey, orderstatus, totalprice, 'NEW' AS orderpriority,
                       'new_clerk' AS clerk, 'new_clerk' AS new_clerk
                FROM tpch.tiny.orders WHERE custkey %% 5 = 0 AND orderkey %% 10 = 0
                """, sourceTable),
                sourceRowCount);
    }

    @AfterAll
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
    }

    private Session scalingEnabledSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(SCALE_WRITERS, "true")
                .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "true")
                .setSystemProperty(WRITER_SCALING_MIN_DATA_PROCESSED, "0B")
                .build();
    }

    @Test
    public void testMergeWriterScalingOnUnpartitionedMoR()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'merge-on-read'");
    }

    @Test
    public void testMergeWriterScalingOnPartitionedMoR()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'merge-on-read', partitioning = ARRAY['orderstatus']");
    }

    @Test
    public void testMergeWriterScalingOnUnpartitionedMoRV3()
    {
        // Iceberg V3 uses deletion vectors
        testMergeWriterScalingCorrectness("merge_mode = 'merge-on-read', format_version = 3");
    }

    @Test
    public void testMergeWriterScalingOnPartitionedMoRV3()
    {
        // Iceberg V3 uses deletion vectors
        testMergeWriterScalingCorrectness("merge_mode = 'merge-on-read', partitioning = ARRAY['orderstatus'], format_version = 3");
    }

    @Test
    public void testMergeWriterScalingOnUnpartitionedCoW()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'copy-on-write'");
    }

    @Test
    public void testMergeWriterScalingOnPartitionedCoW()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'copy-on-write', partitioning = ARRAY['orderstatus']");
    }

    @Test
    public void testMergeWriterScalingOnUnpartitionedCoWV3()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'copy-on-write', format_version = 3");
    }

    @Test
    public void testMergeWriterScalingOnPartitionedCoWV3()
    {
        testMergeWriterScalingCorrectness("merge_mode = 'copy-on-write', partitioning = ARRAY['orderstatus'], format_version = 3");
    }

    private void testMergeWriterScalingCorrectness(String tableProperties)
    {
        String suffix = randomNameSuffix();
        String target = "test_merge_scaling_target_" + suffix;

        try {
            // Create target table
            assertUpdate(format("""
                    CREATE TABLE %s
                    WITH (%s)
                    AS SELECT orderkey, custkey, orderstatus, totalprice, orderpriority, clerk
                    FROM tpch.tiny.orders
                    """, target, tableProperties),
                    ordersCount);

            // Run merge with scaling enabled
            String mergeStatement = format("""
                    MERGE INTO %s AS t USING %s AS s
                    ON t.orderkey = s.orderkey
                    WHEN MATCHED AND s.custkey %% 3 = 0
                        THEN UPDATE SET clerk = s.new_clerk, orderpriority = 'UPDATED'
                    WHEN MATCHED AND s.custkey %% 3 = 1
                        THEN DELETE
                    WHEN NOT MATCHED
                        THEN INSERT (orderkey, custkey, orderstatus, totalprice, orderpriority, clerk)
                             VALUES (s.orderkey, s.custkey, s.orderstatus, s.totalprice, s.orderpriority, s.clerk)
                    """, target, sourceTable);
            assertUpdate(scalingEnabledSession(), mergeStatement, deletedCount + updatedCount + insertedCount);

            // Verify row count
            assertThat(query("SELECT COUNT(*) FROM " + target))
                    .matches(format("VALUES BIGINT '%d'", ordersCount - deletedCount + insertedCount));

            // Verify no duplicate orderkeys (as in the CoW duplication bug)
            assertThat(query("SELECT orderkey, COUNT(*) FROM " + target + " GROUP BY orderkey HAVING COUNT(*) > 1"))
                    .returnsEmptyResult();

            // Verify inserts (negative orderkeys)
            assertThat(query(format("SELECT COUNT(*) FROM %s WHERE orderkey < 0", target)))
                    .matches(format("VALUES BIGINT '%d'", insertedCount));

            // Verify updates
            assertThat(query(format("""
                    SELECT COUNT(*) FROM %s
                    WHERE orderpriority = 'UPDATED' AND clerk LIKE 'updated_%%'
                    """, target)))
                    .matches(format("VALUES BIGINT '%d'", updatedCount));

            // Verify deletes (only check non-negative orderkeys to avoid tpch bucket overflow on negative keys)
            assertThat(query(format("""
                    SELECT COUNT(*) FROM %s t
                    WHERE t.orderkey > 0 AND EXISTS (SELECT 1 FROM tpch.tiny.orders o WHERE o.orderkey = t.orderkey AND o.custkey %% 3 = 1)
                    """, target)))
                    .matches("VALUES BIGINT '0'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + target);
        }
    }
}
