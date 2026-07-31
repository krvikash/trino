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

import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaTableName;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractIcebergOnIcebergMvSubstitutionTest
        extends AbstractIcebergMvSubstitutionTest
{
    @Test
    public void testForVersionAsOfNotSubstituted()
    {
        // FOR VERSION AS OF / FOR TIMESTAMP AS OF requests a specific historical snapshot.
        // The MV holds the current data — substituting would silently drop the user's
        // intended version and return current data instead.
        CatalogSchemaTableName tableName = sourceTable("src_time_travel_" + randomNameSuffix());
        CatalogSchemaTableName mvName = mvName("mv_time_travel_");
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id", 1);
            // Snapshot S1: one row
            long snapshotS1 = (long) computeActual(
                    "SELECT max(snapshot_id) FROM %s.%s.\"%s$snapshots\"".formatted(
                            tableName.getCatalogName(),
                            tableName.getSchemaTableName().getSchemaName(),
                            tableName.getSchemaTableName().getTableName())).getOnlyValue();

            assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES 3", 1);
            // Now the table has 3 rows; S1 still references the 1-row snapshot.

            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM " + tableName);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 3);

            Session session = sessionWithSubstitution();
            // Current-snapshot query — substitution is fine here.
            assertSubstituted(session, "SELECT * FROM " + tableName, tableName, mvName);

            // FOR VERSION AS OF S1 must NOT substitute. The MV holds the current 3-row state;
            // the user asked for the historical 1-row state.
            String versionedQuery = "SELECT * FROM " + tableName + " FOR VERSION AS OF " + snapshotS1;
            assertNotSubstituted(session, versionedQuery, tableName);
            assertThat(computeActual(session, "SELECT count(*) FROM " + tableName + " FOR VERSION AS OF " + snapshotS1).getOnlyValue())
                    .as("Versioned query must return the historical snapshot's data, not the MV's current data")
                    .isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
