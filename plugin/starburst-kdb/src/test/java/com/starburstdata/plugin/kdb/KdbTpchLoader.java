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

import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class KdbTpchLoader
{
    private static final int TPCH_BATCH_SIZE = 500;

    private KdbTpchLoader() {}

    /**
     * Loads TPC-H tables into kdb+ at scale 0.01, recreating them if already present.
     * Uses batched upserts to avoid oversized expressions.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void loadTpchTables(KdbClient client, Iterable<TpchTable<?>> tables)
    {
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            List columns = table.getColumns();
            int numColumns = columns.size();

            List<Object[]> rows = new ArrayList<>();
            for (TpchEntity entity : table.createGenerator(0.01, 1, 1)) {
                Object[] row = new Object[numColumns];
                for (int col = 0; col < numColumns; col++) {
                    @SuppressWarnings("unchecked")
                    TpchColumn<TpchEntity> tpchCol = (TpchColumn<TpchEntity>) columns.get(col);
                    row[col] = switch (tpchCol.getType().getBase()) {
                        case IDENTIFIER -> tpchCol.getIdentifier(entity);
                        case INTEGER -> tpchCol.getInteger(entity);
                        case DOUBLE -> tpchCol.getDouble(entity);
                        case VARCHAR -> tpchCol.getString(entity);
                        // getDate() returns days since 1970-01-01; convert to kdb+ epoch (2000-01-01)
                        case DATE -> tpchCol.getDate(entity) - KdbTypeMapping.KDB_EPOCH_DAYS_OFFSET;
                    };
                }
                rows.add(row);
            }

            client.execute(buildEmptyTableExpr(tableName, columns));

            int total = rows.size();
            for (int start = 0; start < total; start += TPCH_BATCH_SIZE) {
                int end = Math.min(start + TPCH_BATCH_SIZE, total);
                client.execute(buildUpsertExpr(tableName, columns, rows.subList(start, end)));
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static String buildEmptyTableExpr(String tableName, List columns)
    {
        StringBuilder q = new StringBuilder(tableName).append(":([]");
        boolean first = true;
        for (Object colObj : columns) {
            TpchColumn col = (TpchColumn) colObj;
            if (!first) {
                q.append("; ");
            }
            first = false;
            String emptyVal = switch (col.getType().getBase()) {
                case IDENTIFIER -> "`long$()";
                case INTEGER -> "`int$()";
                case DOUBLE -> "`float$()";
                case VARCHAR -> "0#enlist \"\"";
                case DATE -> "`date$()";
            };
            q.append(col.getSimplifiedColumnName()).append(":").append(emptyVal);
        }
        return q.append(")").toString();
    }

    @SuppressWarnings("rawtypes")
    private static String buildUpsertExpr(String tableName, List columns, List<Object[]> rows)
    {
        StringBuilder q = new StringBuilder("`").append(tableName).append(" upsert ([]");
        boolean firstCol = true;
        int numColumns = columns.size();
        for (int col = 0; col < numColumns; col++) {
            TpchColumn tpchCol = (TpchColumn) columns.get(col);
            if (!firstCol) {
                q.append("; ");
            }
            firstCol = false;
            q.append(tpchCol.getSimplifiedColumnName()).append(":");
            appendQValues(q, tpchCol.getType().getBase(), col, rows);
        }
        return q.append(")").toString();
    }

    private static void appendQValues(StringBuilder q, TpchColumnType.Base type, int col, List<Object[]> rows)
    {
        switch (type) {
            case IDENTIFIER -> {
                q.append(rows.stream().map(r -> r[col].toString()).collect(Collectors.joining(" ")));
                q.append("j");
            }
            case INTEGER -> {
                q.append(rows.stream().map(r -> r[col].toString()).collect(Collectors.joining(" ")));
                q.append("i");
            }
            case DOUBLE -> q.append(rows.stream().map(r -> r[col].toString()).collect(Collectors.joining(" ")));
            case VARCHAR -> {
                q.append("(");
                boolean first = true;
                for (Object[] row : rows) {
                    if (!first) {
                        q.append(";");
                    }
                    first = false;
                    q.append("\"").append(escapeQString((String) row[col])).append("\"");
                }
                q.append(")");
            }
            case DATE -> {
                q.append("`date$(");
                q.append(rows.stream().map(r -> r[col].toString()).collect(Collectors.joining(";", "", "i)")));
            }
        }
    }

    private static String escapeQString(String s)
    {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
