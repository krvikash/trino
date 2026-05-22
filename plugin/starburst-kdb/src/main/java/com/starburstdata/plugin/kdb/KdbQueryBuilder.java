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
package com.starburstdata.plugin.kdb;

import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.starburstdata.plugin.kdb.KdbClient.getFullTableName;
import static com.starburstdata.plugin.kdb.KdbClient.validateIdentifier;

public final class KdbQueryBuilder
{
    private KdbQueryBuilder() {}

    public static String buildQuery(SchemaTableName tableName, List<KdbColumnHandle> columns)
    {
        validateIdentifier(tableName.getSchemaName(), "schema");
        validateIdentifier(tableName.getTableName(), "table");
        String fullTableName = getFullTableName(tableName);

        if (columns.isEmpty()) {
            // Columns can be empty for aggregation queries like SELECT COUNT(*)
            return "select from %s".formatted(fullTableName);
        }

        // Use functional select form to safely handle reserved column names
        // ?[table; (); 0b; cols!cols] projects named columns
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                cols.append("`");
            }
            String columnName = columns.get(i).columnName();
            validateIdentifier(columnName, "column");
            cols.append(columnName);
        }
        String colList = columns.size() == 1 ? "enlist[`" + cols + "]" : "`" + cols;
        return "?[%s; (); 0b; %s!%s]".formatted(fullTableName, colList, colList);
    }
}
