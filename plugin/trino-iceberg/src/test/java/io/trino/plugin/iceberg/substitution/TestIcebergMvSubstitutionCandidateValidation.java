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
package io.trino.plugin.iceberg.substitution;

import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Enabling {@code substitution_enabled} on a materialized view that could never be used as a substitution
 * target must fail the DDL rather than leave the property set but inert. The two rejected cases are a
 * definition query reading from a connector without substitution support, and a definition query whose plan
 * shape the materialization IR cannot represent.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionCandidateValidation
        extends AbstractTestQueryFramework
{
    private static final String NOT_A_CANDIDATE = ".*cannot be enabled for substitution: its definition query is not supported by materialized view substitution";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(ICEBERG_CATALOG)
                                .setSchema("test_schema")
                                .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            // A catalog with no ConnectorSubstitutionMetadata, so a scan over it has no identity to persist.
            // Deliberately not tpch: that connector supports substitution.
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory", Map.of());

            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.execute("CREATE SCHEMA iceberg.test_schema");
            queryRunner.execute("CREATE TABLE base_table AS SELECT 1 AS id, 'a' AS name");
            queryRunner.execute("CREATE TABLE memory.default.unsupported_src AS SELECT BIGINT '1' AS id");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Test
    public void testCreateOverUnsupportedConnectorFails()
    {
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_unsupported_connector WITH (substitution_enabled = true) AS SELECT id FROM memory.default.unsupported_src",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_unsupported_connector'");
    }

    @Test
    public void testCreateWithUnsupportedPlanShapeFails()
    {
        // random() is deliberately chosen as the unsupported plan: unlike an aggregation or a plain projection,
        // it can never become supported, because the query has to evaluate it afresh while the materialized
        // view holds values frozen at refresh time.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_non_deterministic WITH (substitution_enabled = true) AS SELECT random() AS r FROM base_table",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_non_deterministic'");
    }

    @Test
    public void testCreateSupportedDefinitionSucceeds()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_supported WITH (substitution_enabled = true) AS SELECT id, name FROM base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_supported");
    }

    @Test
    public void testCreateWithSubstitutionDisabledIsNotValidated()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_disabled WITH (substitution_enabled = false) AS SELECT id FROM memory.default.unsupported_src");
        assertUpdate("DROP MATERIALIZED VIEW mv_disabled");
    }

    @Test
    public void testCreateOrReplaceWithUnsupportedPlanShapeFails()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_replace WITH (substitution_enabled = true) AS SELECT id FROM base_table");
        try {
            assertQueryFails(
                    "CREATE OR REPLACE MATERIALIZED VIEW mv_replace WITH (substitution_enabled = true) AS SELECT id, random() AS r FROM base_table",
                    NOT_A_CANDIDATE);
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_replace"))
                    .as("The rejected replacement must not have been applied")
                    .contains("SELECT id")
                    .doesNotContain("random");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_replace");
        }
    }

    @Test
    public void testSetPropertiesEnablingSubstitutionOnUnsupportedDefinitionFails()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter WITH (substitution_enabled = false) AS SELECT id, random() AS r FROM base_table");
        try {
            assertQueryFails(
                    "ALTER MATERIALIZED VIEW mv_alter SET PROPERTIES substitution_enabled = true",
                    NOT_A_CANDIDATE);
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_alter"))
                    .as("The rejected property change must not have been applied")
                    .contains("substitution_enabled = false");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter");
        }
    }

    @Test
    public void testSetPropertiesDisablingSubstitutionIsNotValidated()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter_disable WITH (substitution_enabled = false) AS SELECT id, random() AS r FROM base_table");
        try {
            assertUpdate("ALTER MATERIALIZED VIEW mv_alter_disable SET PROPERTIES substitution_enabled = false");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter_disable");
        }
    }

    @Test
    public void testSetPropertiesEnablingSubstitutionOnSupportedDefinitionSucceeds()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter_enable WITH (substitution_enabled = false) AS SELECT id FROM base_table");
        try {
            assertUpdate("ALTER MATERIALIZED VIEW mv_alter_enable SET PROPERTIES substitution_enabled = true");
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_alter_enable"))
                    .contains("substitution_enabled = true");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter_enable");
        }
    }
}
