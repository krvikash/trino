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

import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.security.MultipleSystemAccessControlsQueryRunner.ALPHA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.BETA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.GAMMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.SCHEMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.createCatalogs;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.fileAccessControl;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.readOnlyAccessControl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestMultipleSystemAccessControls
{
    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        queryRunner = MultipleSystemAccessControlsQueryRunner.builder(fileAccessControl(), readOnlyAccessControl()).build();
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
    void testOnlyCatalogsEveryAccessControlShowsAreVisible()
    {
        // filterCatalogs chains across both controls
        // the file filter only shows alpha, while the read all allows all
        assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet())
                .contains(ALPHA)
                .doesNotContain(BETA, GAMMA);
    }

    @Test
    void testFileBasedControlDeniesTheCatalogsOutsideItsRules()
    {
        // The read-only control permits reading every catalog, and is overruled
        assertThatThrownBy(() -> queryRunner.execute("SHOW SCHEMAS FROM " + BETA))
                .hasMessageContaining("Access Denied");
        assertThatThrownBy(() -> queryRunner.execute("SHOW SCHEMAS FROM " + GAMMA))
                .hasMessageContaining("Access Denied");
    }

    @Test
    void testReadOnlyControlDeniesWritesToTheCatalogTheFileRulesAllow()
    {
        // The file-based control grants ownership of every schema and table in alpha, and is overruled by the read-only one
        assertThatThrownBy(() -> queryRunner.execute("CREATE TABLE %s.%s.denied AS SELECT 1 x".formatted(ALPHA, SCHEMA)))
                .hasMessageContaining("Access Denied");
        assertThatThrownBy(() -> queryRunner.execute("CREATE SCHEMA %s.denied".formatted(ALPHA)))
                .hasMessageContaining("Access Denied");
    }

    @Test
    void testOperationsThatNameNoCatalogRunThroughEveryAccessControlToo()
    {
        // The read-only control permits setting any system session property, and the file-based
        // one has a rule refusing this particular property
        assertThatThrownBy(() -> queryRunner.execute("SET SESSION query_max_run_time = '42m'"))
                .hasMessageContaining("Access Denied");

        // A property the file-based rules do allow is permitted by both, and so is allowed
        queryRunner.execute("SET SESSION query_max_planning_time = '42m'");
    }
}
