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
package io.trino.plugin.google.sheets.substitution;

import io.trino.plugin.google.sheets.SheetsQueryRunner;
import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;

/**
 * MV substitution over a Google Sheets source. Google Sheets is read-only and its named tables come
 * from a fixed metadata sheet, so it uses the read-only suite over the always-present test tables
 * {@code number_text}/{@code metadata_table} (all columns are VARCHAR). Requires Google API
 * credentials, so it runs under the {@code cloud-tests} profile (see pom surefire config).
 */
public class TestIcebergMvOnGoogleSheetsSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        return SheetsQueryRunner.builder()
                .addConnectorProperty("gsheets.metadata-sheet-id", TEST_METADATA_SHEET_ID)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName("gsheets", "default", "number_text");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName("gsheets", "default", "metadata_table");
    }

    @Override
    protected String materializedColumn()
    {
        return "number";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "text";
    }
}
