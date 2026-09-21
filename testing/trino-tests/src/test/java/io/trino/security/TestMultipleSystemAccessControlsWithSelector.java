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
package io.trino.security;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static io.trino.security.MultipleSystemAccessControlsQueryRunner.ALPHA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.BETA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.GAMMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.IMPERSONATED_USER;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.SCHEMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.createCatalogs;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.fileAccessControl;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.readOnlyAccessControl;
import static io.trino.security.SystemAccessControlOperation.SYSTEM_SESSION_PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestMultipleSystemAccessControlsWithSelector
{
    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        SystemAccessControl fileAccessControl = fileAccessControl();
        SystemAccessControl readOnlyAccessControl = readOnlyAccessControl();
        Map<String, SystemAccessControl> catalogAccessControls = ImmutableMap.of(
                ALPHA, fileAccessControl,
                BETA, readOnlyAccessControl);

        queryRunner = MultipleSystemAccessControlsQueryRunner.builder(fileAccessControl, readOnlyAccessControl)
                .setSystemAccessControlSelector(new SystemAccessControlSelector()
                {
                    @Override
                    public boolean governsCatalog(SystemAccessControl accessControl, String catalogName)
                    {
                        return isSelected(accessControl, catalogAccessControls.get(catalogName));
                    }

                    @Override
                    public boolean governsOperation(SystemAccessControl accessControl, SystemAccessControlOperation operation)
                    {
                        if (operation == SYSTEM_SESSION_PROPERTIES) {
                            return isSelected(accessControl, readOnlyAccessControl);
                        }
                        return true;
                    }
                })
                .build();
        createCatalogs(queryRunner);
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    void testEveryCatalogWithASelectionIsVisible()
    {
        // Alpha is visible because there are rules for it in the file access control
        // Beta is visible because READ ALL is used for that access control
        // Gamma uses all access controls, but is not visible because there are no rules in the file for it
        assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet())
                .containsOnly(ALPHA, BETA, "system")
                .doesNotContain(GAMMA);
    }

    @Test
    void testTheCatalogSelectingTheFileBasedControlIsWritable()
    {
        // The read-only control would have denied this
        queryRunner.execute("CREATE TABLE %s.%s.owned_by_file AS SELECT 1 x".formatted(ALPHA, SCHEMA));

        assertThat(queryRunner.execute("SELECT x FROM %s.%s.owned_by_file".formatted(ALPHA, SCHEMA)).getOnlyValue())
                .isEqualTo(1);
        assertThat(queryRunner.execute("SHOW TABLES FROM %s.%s".formatted(ALPHA, SCHEMA)).getOnlyColumnAsSet())
                .containsExactly("owned_by_file");

        // Write operations are denied in Beta
        assertThatThrownBy(() -> queryRunner.execute("CREATE SCHEMA %s.denied".formatted(BETA)))
                .hasMessage("Access Denied: Cannot create schema %s.denied".formatted(BETA));
        assertThatThrownBy(() -> queryRunner.execute("CREATE TABLE %s.%s.denied AS SELECT 1 x".formatted(BETA, SCHEMA)))
                .hasMessage("Access Denied: Cannot create table %s.%s.denied".formatted(BETA, SCHEMA));
    }

    @Test
    void testTheCatalogSelectingTheReadOnlyControlIsReadable()
    {
        // The file-based control has no rule covering beta and thus denied all of this before routing.
        // The read-only control filters nothing out, and is now the only one asked, so every
        // schema, relation and column beta has comes back in full.
        assertThat(queryRunner.execute("SHOW SCHEMAS FROM " + BETA).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(SCHEMA, "information_schema");
        assertThat(queryRunner.execute("SELECT schema_name FROM %s.information_schema.schemata".formatted(BETA)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(SCHEMA, "information_schema");

        // Make sure queries work on actual tables in read-only access control mode
        assertThat(queryRunner.execute("SELECT table_schema || '.' || table_name FROM %s.information_schema.tables".formatted(BETA)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(
                        "information_schema.applicable_roles",
                        "information_schema.columns",
                        "information_schema.enabled_roles",
                        "information_schema.roles",
                        "information_schema.schemata",
                        "information_schema.table_privileges",
                        "information_schema.tables",
                        "information_schema.views");

        // Columns are routed to the control selected for beta the same way, and survive filterColumns intact
        assertThat(queryRunner.execute("SELECT column_name FROM %s.information_schema.columns WHERE table_name = 'tables'".formatted(BETA)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("table_catalog", "table_schema", "table_name", "table_type");

        // Every one of those relations is readable, and the ones describing relations beta does not
        // have are empty rather than denied
        assertThat(queryRunner.execute("SELECT * FROM %s.information_schema.views".formatted(BETA)).getRowCount()).isZero();
        assertThat(queryRunner.execute("SELECT * FROM %s.information_schema.table_privileges".formatted(BETA)).getRowCount()).isZero();
        assertThat(queryRunner.execute("SELECT * FROM %s.information_schema.roles".formatted(BETA)).getRowCount()).isZero();
        assertThat(queryRunner.execute("SELECT * FROM %s.information_schema.applicable_roles".formatted(BETA)).getRowCount()).isZero();
        assertThat(queryRunner.execute("SELECT * FROM %s.information_schema.enabled_roles".formatted(BETA)).getRowCount()).isZero();

        // Reading beta on its own succeeds, and reading gamma on its own does not, so a query
        // reading both must be routed catalog by catalog rather than once for the whole statement
        assertThat(queryRunner.execute("SELECT count(*) FROM %s.information_schema.tables".formatted(BETA)).getOnlyValue())
                .isEqualTo(8L);
        assertThatThrownBy(() -> queryRunner.execute(
                "SELECT count(*) FROM %s.information_schema.tables UNION ALL SELECT count(*) FROM %s.information_schema.tables".formatted(BETA, GAMMA)))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);

        // Gamma is configured exactly like beta and differs only in having nothing selected for it,
        // which leaves it to every configured control. Each of these queries is one that runs
        // against beta and gets an answer for, and the read-only control would allow every one of
        // them here too. They fail because the file-based control is still asked about gamma, which
        // is outside its rules, and refuses at the catalog itself before any relation is considered.
        assertThatThrownBy(() -> queryRunner.execute("SHOW SCHEMAS FROM " + GAMMA))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);

        assertThatThrownBy(() -> queryRunner.execute("SELECT schema_name FROM %s.information_schema.schemata".formatted(GAMMA)))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);
        assertThatThrownBy(() -> queryRunner.execute("SELECT table_schema || '.' || table_name FROM %s.information_schema.tables".formatted(GAMMA)))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);
        assertThatThrownBy(() -> queryRunner.execute("SELECT column_name FROM %s.information_schema.columns WHERE table_name = 'tables'".formatted(GAMMA)))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);
        assertThatThrownBy(() -> queryRunner.execute("SHOW TABLES FROM %s.%s".formatted(GAMMA, SCHEMA)))
                .hasMessage("Access Denied: Cannot access catalog " + GAMMA);
    }

    @Test
    void testAnOperationWithNoSelectionStillRunsThroughEveryAccessControl()
    {
        // The file-based control has a rule refusing this property, and no longer governs the
        // setting of system session properties at all
        queryRunner.execute("SET SESSION query_max_run_time = '42m'");

        // This selector narrows the controls for setting system session properties and for nothing
        // else, so user impersonation is left to every configured control and keeps the behaviour it
        // has when no selector is bound at all: the file-based control's rules permit exactly this
        // impersonation, the read-only control refuses it, and the refusal wins.
        assertThatThrownBy(() -> queryRunner.execute("SET SESSION AUTHORIZATION " + IMPERSONATED_USER))
                .hasMessageContaining("Access Denied")
                .hasMessageContaining("cannot impersonate user " + IMPERSONATED_USER);
    }

    private static boolean isSelected(SystemAccessControl accessControl, SystemAccessControl selected)
    {
        return selected == null || selected == accessControl;
    }
}
