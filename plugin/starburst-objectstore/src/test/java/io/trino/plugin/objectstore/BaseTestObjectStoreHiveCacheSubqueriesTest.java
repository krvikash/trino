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
package io.trino.plugin.objectstore;

import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.testing.BaseCacheSubqueriesTest;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public abstract class BaseTestObjectStoreHiveCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
{
    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioned_by=array[%s]) as %s",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                asSelect);

        getQueryRunner().execute(sql);
    }

    @Override
    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return Session.builder(session)
                .setSystemProperty("objectstore.projection_pushdown_enabled", String.valueOf(projectionPushdownEnabled))
                .build();
    }

    protected Plugin getObjectStorePlugin()
    {
        return new ObjectStorePlugin();
    }

    @Override
    protected boolean supportsDataColumnPruning()
    {
        return false;
    }
}
