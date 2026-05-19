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
package io.trino.plugin.hive.ozone;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestHiveConnectorSmokeTest;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHiveOzoneS3GatewayConnectorSmokeTest
        extends TestHiveConnectorSmokeTest
{
    private static final String CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "ozone";

    private String bucketName;
    protected HiveOzoneS3Gateway hiveOzoneS3Gateway;

    protected abstract Map<String, String> s3Config();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        bucketName = "test-hive-ozone-smoke-test" + randomNameSuffix();

        hiveOzoneS3Gateway = closeAfterClass(new HiveOzoneS3Gateway(bucketName));

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setMetastore(_ -> new BridgingHiveMetastore(
                        testingThriftHiveMetastoreBuilder()
                                .metastoreClient(hiveOzoneS3Gateway.getHiveHadoop().getHiveMetastoreEndpoint(), TestingTokenAwareMetastoreClientFactory.TIMEOUT)
                                .build(this::closeAfterClass)))
                .amendSession(sessionBuilder -> sessionBuilder.setCatalog(CATALOG_NAME).setSchema(SCHEMA_NAME))
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.security", "allow-all")
                        .putAll(s3Config())
                        .buildOrThrow())
                .build();

        queryRunner.execute(createSchemaSql(SCHEMA_NAME));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), ImmutableList.<TpchTable<?>>builder()
                .addAll(REQUIRED_TPCH_TABLES)
                .add(LINE_ITEM)
                .build());

        return queryRunner;
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        """
                        CREATE TABLE hive.ozone.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           format = 'ORC'
                        )""");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.ofUser("ADMIN"))
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        assertThat(query(newSession, "SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s'", schemaName));
        assertUpdate(newSession, "DROP SCHEMA " + schemaName);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", SCHEMA_NAME, SCHEMA_NAME + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }
}
