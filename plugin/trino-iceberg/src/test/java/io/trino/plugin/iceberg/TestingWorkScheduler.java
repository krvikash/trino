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

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class TestingWorkScheduler
        implements WorkScheduler
{
    private final Map<String, MaterializedViewRefresh> jobs = new HashMap<>();
    private final Supplier<QueryRunner> queryRunner;

    public TestingWorkScheduler(Supplier<QueryRunner> queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    @Override
    public synchronized String createMaterializedViewRefreshJob(ConnectorSession session, String catalogName, String schemaName, String materializedViewName, String jobCron)
    {
        String id = UUID.randomUUID().toString();
        jobs.put(id, new MaterializedViewRefresh(new CatalogSchemaTableName(catalogName, schemaName, materializedViewName), jobCron));
        return id;
    }

    @Override
    public synchronized Optional<String> getJobSchedule(ConnectorSession session, String jobId)
    {
        return Optional.ofNullable(jobs.get(jobId)).map(MaterializedViewRefresh::jobCron);
    }

    @Override
    public synchronized void deleteJobSchedule(ConnectorSession session, String jobId)
    {
        jobs.remove(jobId);
    }

    @Override
    public synchronized boolean updateJobSchedule(ConnectorSession session, String jobId, String jobCron)
    {
        return jobs.computeIfPresent(jobId, (key, existing) -> existing.withCronJob(jobCron)) != null;
    }

    @Override
    public synchronized boolean updateMaterializedViewName(ConnectorSession session, String jobId, String materializedViewName)
    {
        return jobs.computeIfPresent(jobId, (key, existing) -> existing.withMaterializedViewName(materializedViewName)) != null;
    }

    public synchronized void runScheduledRefreshesForJobId(String jobId)
    {
        refreshMaterializedView(jobs.get(jobId));
    }

    public synchronized String getRequiredJobScheduleId(CatalogSchemaTableName materializedView)
    {
        return getJobScheduleId(materializedView).orElseThrow();
    }

    public synchronized Optional<String> getJobScheduleId(CatalogSchemaTableName materializedView)
    {
        return jobs.entrySet().stream()
                .filter(entry -> entry.getValue().table().equals(materializedView))
                .map(Map.Entry::getKey)
                .findAny();
    }

    private MaterializedResult refreshMaterializedView(MaterializedViewRefresh job)
    {
        return queryRunner.get().execute("REFRESH MATERIALIZED VIEW " + job.table());
    }

    private record MaterializedViewRefresh(CatalogSchemaTableName table, String jobCron)
    {
        private MaterializedViewRefresh
        {
            requireNonNull(table, "table is null");
            requireNonNull(jobCron, "jobCron is null");
        }

        public MaterializedViewRefresh withCronJob(String jobCron)
        {
            return new MaterializedViewRefresh(table, jobCron);
        }

        public MaterializedViewRefresh withMaterializedViewName(String materializedViewName)
        {
            return new MaterializedViewRefresh(new CatalogSchemaTableName(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), materializedViewName), jobCron);
        }
    }
}
