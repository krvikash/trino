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
package io.trino.plugin.bigquery;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestBigQueryWithCommittedWriterConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(Map.of("bigquery.write-stream-type", "COMMITTED"))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        // https://starburstdata.atlassian.net/browse/ENG-8251
        // there is a brief delay before newly written data becomes visible in the BigQuery connector
        Failsafe.with(RetryPolicy.builder().withMaxAttempts(3).build())
                .run(super::testRowLevelDelete);
    }
}
