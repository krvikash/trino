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
package io.starburst.stargate.tablemaintenance;

import io.airlift.units.DataSize;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.List;
import java.util.stream.Collectors;

public class MaintenanceQueryGenerators
{
    private MaintenanceQueryGenerators() {}

    public static String buildAnalyzeQuery(CatalogSchemaTableName tableName, List<String> onlyColumns)
    {
        String withColumns = onlyColumns.isEmpty() ? "" :
                """
                WITH (
                columns = ARRAY[%s])""".formatted(onlyColumns.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", ")));
        String analyzeTable =
                """
                ANALYZE "%s"."%s"."%s" %s""";
        return analyzeTable.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                withColumns);
    }

    public static String buildAlterTable(CatalogSchemaTableName tableName, String function)
    {
        return """
               ALTER TABLE "%s"."%s"."%s" EXECUTE %s
               """.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                function);
    }

    public static String buildDeltaDropExtendedStatsQuery(CatalogSchemaTableName tableName)
    {
        return """
               CALL "%s".system.drop_extended_stats('%s', '%s')"""
                .formatted(
                        doubleQuoteEscape(tableName.getCatalogName()),
                        singleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                        singleQuoteEscape(tableName.getSchemaTableName().getTableName()));
    }

    public static String buildDeltaVacuumQuery(CatalogSchemaTableName tableName, int retentionThresholdDays)
    {
        return """
               CALL "%s".system.vacuum('%s', '%s', '%dd')"""
                .formatted(
                        doubleQuoteEscape(tableName.getCatalogName()),
                        singleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                        singleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                        retentionThresholdDays);
    }

    static String buildOptimizeTableWithPredicate(CatalogSchemaTableName tableName, String predicate)
    {
        return """
               ALTER TABLE "%s"."%s"."%s" EXECUTE OPTIMIZE
               %s
               """.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                predicate);
    }

    static String buildOptimizeTable(CatalogSchemaTableName tableName, DataSize fileSizeThreshold, String predicate)
    {
        return """
               ALTER TABLE "%s"."%s"."%s" EXECUTE OPTIMIZE (file_size_threshold => '%s')
               %s
               """.formatted(
                doubleQuoteEscape(tableName.getCatalogName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getSchemaName()),
                doubleQuoteEscape(tableName.getSchemaTableName().getTableName()),
                fileSizeThreshold.toString(),
                predicate);
    }

    public static String doubleQuoteEscape(String str)
    {
        return str.replaceAll("\"", "\"\"");
    }

    public static String singleQuoteEscape(String str)
    {
        return str.replaceAll("'", "''");
    }
}
