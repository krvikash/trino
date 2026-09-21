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

import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.security.MultipleSystemAccessControlsQueryRunner.ALPHA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.GAMMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.SCHEMA;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.createCatalogs;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.fileAccessControl;
import static io.trino.security.MultipleSystemAccessControlsQueryRunner.readOnlyAccessControl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestUngovernedCatalogWithSelector
{
    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        SystemAccessControl fileAccessControl = fileAccessControl();
        SystemAccessControl readOnlyAccessControl = readOnlyAccessControl();

        queryRunner = MultipleSystemAccessControlsQueryRunner.builder(fileAccessControl, readOnlyAccessControl)
                .setSystemAccessControlSelector(new SystemAccessControlSelector()
                {
                    @Override
                    public boolean governsCatalog(SystemAccessControl accessControl, String catalogName)
                    {
                        return !catalogName.equals(GAMMA);
                    }

                    @Override
                    public boolean governsOperation(SystemAccessControl accessControl, SystemAccessControlOperation operation)
                    {
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
    void testACatalogNothingGovernsIsLeftUnchecked()
    {
        // No configured access control governs gamma, so nothing is consulted about it and every
        // check on it passes. The file-based control would otherwise have hidden it, as it does for
        // the catalogs it still governs
        assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet())
                .contains(ALPHA, GAMMA);

        assertThat(queryRunner.execute("SHOW SCHEMAS FROM " + GAMMA).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(SCHEMA, "information_schema");

        // The read-only control governs neither gamma's reads nor its writes, so even a write is allowed
        queryRunner.execute("CREATE TABLE %s.%s.ungoverned AS SELECT 1 x".formatted(GAMMA, SCHEMA));
        assertThat(queryRunner.execute("SELECT x FROM %s.%s.ungoverned".formatted(GAMMA, SCHEMA)).getOnlyValue())
                .isEqualTo(1);
    }
}
