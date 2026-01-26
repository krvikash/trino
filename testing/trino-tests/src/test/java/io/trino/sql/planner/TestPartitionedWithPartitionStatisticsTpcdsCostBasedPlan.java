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

package io.trino.sql.planner;

import io.trino.tpcds.Table;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * This class tests cost-based optimization rules. It contains unmodified TPC-DS queries.
 * This class is using Iceberg connector partitioned TPC-DS tables.
 */
public class TestPartitionedWithPartitionStatisticsTpcdsCostBasedPlan
        extends BaseCostBasedPlanTest
{
    protected TestPartitionedWithPartitionStatisticsTpcdsCostBasedPlan()
    {
        super("tpcds_sf1000_parquet_part_with_part_stats", true);
    }

    @Override
    protected List<String> getTableNames()
    {
        return Table.getBaseTables().stream()
                .filter(table -> table != Table.DBGEN_VERSION)
                .map(Table::getName)
                .collect(toImmutableList());
    }

    @Override
    protected String getQueryPlanResourcePath(String queryResourcePath)
    {
        Path queryPath = Path.of(queryResourcePath);
        Path directory = queryPath.getParent();
        directory = directory.resolve("iceberg").resolve("partitioned_with_part_stats");
        String planResourceName = queryPath.getFileName().toString().replaceAll("\\.sql$", ".plan.txt");
        return directory.resolve(planResourceName).toString();
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpcds/sf1000/partitioned_with_part_stats/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpcds-sf1000-parquet-part-with-part-stats/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCDS_SQL_FILES;
    }

    public static void main(String[] args)
    {
        new TestPartitionedWithPartitionStatisticsTpcdsCostBasedPlan().generate();
    }
}
