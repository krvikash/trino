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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.ozone.HiveOzoneS3Gateway;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO extends from BaseDeltaLakeConnectorSmokeTest https://starburstdata.atlassian.net/browse/SEP-13328
public abstract class BaseDeltaLakeOzoneConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private final String bucketName = "test-delta-lake-ozone-integration-smoke-test-" + randomNameSuffix();

    protected abstract Map<String, String> s3Config(HiveOzoneS3Gateway hiveOzoneS3Gateway);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String schemaName = "ozone";

        HiveOzoneS3Gateway hiveOzoneS3Gateway = closeAfterClass(new HiveOzoneS3Gateway(bucketName));

        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(schemaName)
                .setSchemaLocation(format("s3://%s/%s", bucketName, schemaName))
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("delta.metadata.cache-ttl", "15s")
                        .put("hive.metastore-cache-ttl", "15s")
                        .put("delta.enable-non-concurrent-writes", "true")
                        .put("delta.register-table-procedure.enabled", "true")
                        .put("hive.metastore.uri", hiveOzoneS3Gateway.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                        .putAll(s3Config(hiveOzoneS3Gateway))
                        .buildOrThrow())
                .build();

        queryRunner.execute(createSchemaSql(schemaName));

        REQUIRED_TPCH_TABLES.forEach(table -> queryRunner.execute(format(
                "CREATE TABLE %s WITH (location = '%s') AS SELECT * FROM tpch.tiny.%1$s",
                table.getTableName(),
                "s3://%s/%s".formatted(bucketName, table.getTableName()))));

        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
            case SUPPORTS_TRUNCATE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE %s.ozone.region (
                           regionkey bigint,
                           name varchar,
                           comment varchar
                        )
                        WITH (
                           location = 's3://%s/region'
                        )""".formatted(getSession().getCatalog().orElseThrow(), bucketName));
    }
}
