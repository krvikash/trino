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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.MAX_WRITER_TASK_COUNT;
import static io.trino.SystemSessionProperties.PARTIAL_LIMIT_HINT_ENABLED;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSortedWriting
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.LINE_ITEM))
                .addIcebergProperty("iceberg.sorted-writing-enabled", "true")
                // Test staging of sorted writes to local disk
                .addIcebergProperty("iceberg.sorted-writing.local-staging-path", "/tmp/trino-${USER}")
                // Allows testing the sorting writer flushing to the file system with smaller tables
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .build();
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Test
    public void testSortedWritingWithLocalStaging()
    {
        testSortedWritingWithLocalStaging(FileFormat.ORC);
        testSortedWritingWithLocalStaging(FileFormat.PARQUET);
    }

    private void testSortedWritingWithLocalStaging(FileFormat format)
    {
        // Using a larger table forces buffered data to be written to disk
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "200")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "20kB")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "200")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups,
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem",
                    "VALUES 60175");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted(Location.of((String) filePath), "comment", format)).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }

    @Test
    public void testPreSortedInput()
    {
        // Using a small file size forces multiple files to be created
        Session withSmallFileSize = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "target_max_file_size", "20kB")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['orderkey ASC NULLS FIRST', 'linenumber ASC NULLS FIRST'], format = '" + PARQUET + "') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate(
                    withSmallFileSize,
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem",
                    "VALUES 60175");
            int filesCount = computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet().size();
            assertThat(filesCount).isGreaterThanOrEqualTo(6);

            // TopNPartial is used by default
            assertThat(
                    query("SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST LIMIT 10"))
                    .matches(anyTree(
                            topN(
                                    10, ImmutableList.of(sort("o", ASCENDING, FIRST)), TopNNode.Step.PARTIAL,
                                    tableScan(
                                            handle -> !((IcebergTableHandle) handle).useSmallReadsPerSplit(),
                                            TupleDomain.all(),
                                            ImmutableMap.of("o", equalTo("orderkey"))))));

            Session withUnsafeSortingProperty = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "unsafe_sorting_properties_enabled", "true")
                    .build();
            // LimitPartial is used when sorting property is enabled
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST LIMIT 10"))
                    .matches(anyTree(
                            limit(
                                    10, ImmutableList.of(), true, ImmutableList.of("o"),
                                    tableScan(
                                            handle -> ((IcebergTableHandle) handle).useSmallReadsPerSplit(),
                                            TupleDomain.all(),
                                            ImmutableMap.of("o", equalTo("orderkey"))))));
            // useSmallReadsPerSplit should be false with large LIMIT
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST LIMIT 100001"))
                    .matches(anyTree(
                            limit(
                                    100001, ImmutableList.of(), true, ImmutableList.of("o"),
                                    tableScan(
                                            handle -> !((IcebergTableHandle) handle).useSmallReadsPerSplit(),
                                            TupleDomain.all(),
                                            ImmutableMap.of("o", equalTo("orderkey"))))));
            // Filter between TopN and Scan
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " WHERE orderkey > 10 ORDER BY orderkey ASC NULLS FIRST LIMIT 10"))
                    .matches(anyTree(
                            limit(
                                    10, ImmutableList.of(), true, ImmutableList.of("o"),
                                    node(
                                            FilterNode.class,
                                            tableScan(
                                                    handle -> ((IcebergTableHandle) handle).useSmallReadsPerSplit(),
                                                    TupleDomain.all(),
                                                    ImmutableMap.of("o", equalTo("orderkey")))))));
            // Multiple sorted columns
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST, linenumber ASC NULLS FIRST LIMIT 10"))
                    .matches(anyTree(
                            limit(
                                    10, ImmutableList.of(), true, ImmutableList.of("o", "l"),
                                    tableScan(
                                            handle -> ((IcebergTableHandle) handle).useSmallReadsPerSplit(),
                                            TupleDomain.all(),
                                            ImmutableMap.of("o", equalTo("orderkey"), "l", equalTo("linenumber"))))));

            // Sorting property mismatch on 2nd column
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST, linenumber LIMIT 10"))
                    .matches(anyTree(
                            topN(
                                    10, ImmutableList.of(sort("o", ASCENDING, FIRST), sort("l", ASCENDING, LAST)), TopNNode.Step.PARTIAL,
                                    tableScan(table.getName(), ImmutableMap.of("o", "orderkey", "l", "linenumber")))));
            // Sorting property mismatch on 1st column
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC LIMIT 10"))
                    .matches(anyTree(
                            topN(
                                    10, ImmutableList.of(sort("o", ASCENDING, LAST)), TopNNode.Step.PARTIAL,
                                    tableScan(table.getName(), ImmutableMap.of("o", "orderkey")))));
            assertThat(
                    query(withUnsafeSortingProperty, "SELECT * FROM " + table.getName() + " ORDER BY orderkey DESC NULLS FIRST LIMIT 10"))
                    .matches(anyTree(
                            topN(
                                    10, ImmutableList.of(sort("o", DESCENDING, FIRST)), TopNNode.Step.PARTIAL,
                                    tableScan(table.getName(), ImmutableMap.of("o", "orderkey")))));

            // Verify results
            assertQuery(
                    withUnsafeSortingProperty,
                    "SELECT * FROM " + table.getName() + " WHERE orderkey BETWEEN 10 AND 14000 ORDER BY orderkey ASC NULLS FIRST LIMIT 100",
                    "SELECT * FROM lineitem WHERE orderkey BETWEEN 10 AND 14000 ORDER BY orderkey ASC LIMIT 100");
        }
    }

    @Test
    public void testSmallReadsPerSplit()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['orderkey ASC NULLS FIRST'], format = '" + PARQUET + "') AS TABLE tpch.sf1.lineitem WITH NO DATA")) {
            assertUpdate(
                    Session.builder(getSession())
                            // disable writer scaling for the test
                            .setSystemProperty(SCALE_WRITERS, "false")
                            .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                            // limit number of writer tasks to 1
                            .setSystemProperty(MAX_WRITER_TASK_COUNT, "1")
                            .build(),
                    "INSERT INTO " + table.getName() + " TABLE tpch.sf1.lineitem",
                    "VALUES 6001215");

            Session withUnsafeSortingProperty = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "unsafe_sorting_properties_enabled", "true")
                    .build();
            Session withoutSmallReadsPerSplit = Session.builder(withUnsafeSortingProperty)
                    .setSystemProperty(PARTIAL_LIMIT_HINT_ENABLED, "false")
                    .build();

            QueryRunner.MaterializedResultWithQueryId resultWithQueryId = getDistributedQueryRunner().executeWithQueryId(
                    withoutSmallReadsPerSplit,
                    "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST LIMIT 10");
            OperatorStats baselineScanStats = getScanOperatorStats(
                    resultWithQueryId.queryId(),
                    getQualifiedTableName(table.getName()));
            assertThat(baselineScanStats.getPhysicalInputDataSize().toBytes()).isGreaterThan(0);

            resultWithQueryId = getDistributedQueryRunner().executeWithQueryId(
                    withUnsafeSortingProperty,
                    "SELECT * FROM " + table.getName() + " ORDER BY orderkey ASC NULLS FIRST LIMIT 10");
            OperatorStats scanStatsWithSmallReads = getScanOperatorStats(
                    resultWithQueryId.queryId(),
                    getQualifiedTableName(table.getName()));
            assertThat(scanStatsWithSmallReads.getPhysicalInputDataSize().toBytes())
                    .isLessThan((long) (0.5 * baselineScanStats.getPhysicalInputDataSize().toBytes()));
        }
    }

    private boolean isFileSorted(Location path, String sortColumnName, FileFormat format)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    private OperatorStats getScanOperatorStats(QueryId queryId, QualifiedObjectName catalogSchemaTableName)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
        TableScanNode planNode = (TableScanNode) PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof TableScanNode scanNode)) {
                        return false;
                    }
                    CatalogSchemaTableName tableName = getTableName(scanNode.getTable());
                    return tableName.equals(catalogSchemaTableName.asCatalogSchemaTableName());
                })
                .findOnlyElement();

        return extractOperatorStatsForNodeId(queryId, planNode.getId(), "TableScanOperator");
    }

    private static Predicate<ColumnHandle> equalTo(String columnName)
    {
        return columnHandle -> ((IcebergColumnHandle) columnHandle).getName().equals(columnName);
    }
}
