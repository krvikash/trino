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
package io.trino.plugin.iceberg.procedure;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.PartitionFields;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.IcebergSchemaUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_ORDINAL_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TIMESTAMP_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TYPE_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_VERSION_NAME;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplit.DELETE_CHANGE_TYPE_VALUE;
import static io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplit.INSERT_CHANGE_TYPE_VALUE;
import static io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplit.UPDATE_AFTER_CHANGE_TYPE_VALUE;
import static io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplit.UPDATE_BEFORE_CHANGE_TYPE_VALUE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.CREATE_CHANGELOG_VIEW;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.util.SnapshotUtil.schemaFor;

public class CreateChangelogView
{
    // Metadata columns emitted by the table_changes table function (see IcebergColumnHandle.DATA_CHANGE_*_NAME).
    // These names are reserved so a changelog view cannot collide with the columns table_changes appends.
    private static final Set<String> RESERVED_CHANGE_COLUMNS = ImmutableSet.of(
            DATA_CHANGE_TYPE_NAME,
            DATA_CHANGE_VERSION_NAME,
            DATA_CHANGE_TIMESTAMP_NAME,
            DATA_CHANGE_ORDINAL_NAME);

    private final String catalogName;
    private final TypeManager typeManager;

    @Inject
    public CreateChangelogView(CatalogName catalogName, TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Optional<ConnectorTableExecuteHandle> getTableHandle(
            ConnectorSession session,
            TrinoCatalog catalog,
            ConnectorAccessControl accessControl,
            IcebergTableHandle tableHandle,
            Map<String, Object> executeProperties)
    {
        SchemaTableName sourceTable = tableHandle.getSchemaTableName();
        String schemaName = sourceTable.getSchemaName();
        String tableName = sourceTable.getTableName();
        SchemaTableName viewName = resolveViewName(executeProperties, sourceTable);

        accessControl.checkCanCreateView(null, viewName);

        Table icebergTable = catalog.loadTable(session, sourceTable);
        validateWriteModes(executeProperties, icebergTable);

        SnapshotRange snapshots = resolveSnapshotRange(executeProperties, icebergTable);
        Schema endSchema = schemaFor(icebergTable, snapshots.endSnapshotId());
        List<String> dataColumnNames = endSchema.columns().stream()
                .map(NestedField::name)
                .collect(toImmutableList());
        validateNoReservedColumnCollisions(dataColumnNames);
        verifyColumnsAreComparable(endSchema);

        List<String> resolvedIdentifierColumns = resolveIdentifierColumns(executeProperties, endSchema);

        Set<String> selectedColumns = ImmutableSet.<String>builder()
                .addAll(dataColumnNames)
                .addAll(RESERVED_CHANGE_COLUMNS)
                .build();
        accessControl.checkCanCreateViewWithSelectFromColumns(null, sourceTable, Optional.empty(), selectedColumns);

        String viewSql = buildViewSql(
                schemaName,
                tableName,
                snapshots.startSnapshotId(),
                snapshots.endSnapshotId(),
                endSchema.columns(),
                resolvedIdentifierColumns);
        List<ViewColumn> viewColumns = buildViewColumns(endSchema);
        String viewComment = format(
                "Generated by create_changelog_view for %s.%s.%s between snapshots %d and %d",
                catalogName,
                schemaName,
                tableName,
                snapshots.startSnapshotId(),
                snapshots.endSnapshotId());

        return Optional.of(new IcebergTableExecuteHandle(
                sourceTable,
                CREATE_CHANGELOG_VIEW,
                new IcebergCreateChangelogViewHandle(viewName, viewSql, viewColumns, catalogName, viewComment),
                icebergTable.location(),
                tableHandle.getFormatVersion()));
    }

    private record SnapshotRange(long startSnapshotId, long endSnapshotId) {}

    private static SnapshotRange resolveSnapshotRange(Map<String, Object> properties, Table icebergTable)
    {
        long startSnapshotId = requiredLongProperty(properties, "start_snapshot_id");
        long endSnapshotId = requiredLongProperty(properties, "end_snapshot_id");
        if (icebergTable.snapshot(endSnapshotId) == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("End snapshot %d not found in table history", endSnapshotId));
        }
        if (icebergTable.snapshot(startSnapshotId) == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("Start snapshot %d not found in table history", startSnapshotId));
        }
        if (!SnapshotUtil.isParentAncestorOf(icebergTable, endSnapshotId, startSnapshotId)) {
            throw new TrinoException(
                    INVALID_PROCEDURE_ARGUMENT,
                    format("Start snapshot %d is not an ancestor of end snapshot %d", startSnapshotId, endSnapshotId));
        }
        return new SnapshotRange(startSnapshotId, endSnapshotId);
    }

    private static SchemaTableName resolveViewName(Map<String, Object> properties, SchemaTableName sourceTable)
    {
        String requestedViewName = (String) properties.get("view_name");
        String requestedSchemaName = (String) properties.get("schema_name");
        String resolvedViewName = (requestedViewName != null && !requestedViewName.isEmpty())
                ? requestedViewName
                : sourceTable.getTableName() + "_changes";
        String resolvedSchemaName = (requestedSchemaName != null && !requestedSchemaName.isEmpty())
                ? requestedSchemaName
                : sourceTable.getSchemaName();
        return new SchemaTableName(resolvedSchemaName, resolvedViewName);
    }

    private static void validateWriteModes(Map<String, Object> properties, Table icebergTable)
    {
        if ((boolean) properties.getOrDefault("skip_write_mode_validation", false)) {
            return;
        }
        Map<String, String> tableProperties = icebergTable.properties();
        checkCopyOnWrite(tableProperties, TableProperties.DELETE_MODE, TableProperties.DELETE_MODE_DEFAULT);
        checkCopyOnWrite(tableProperties, TableProperties.UPDATE_MODE, TableProperties.UPDATE_MODE_DEFAULT);
        checkCopyOnWrite(tableProperties, TableProperties.MERGE_MODE, TableProperties.MERGE_MODE_DEFAULT);
    }

    private static void validateNoReservedColumnCollisions(List<String> dataColumnNames)
    {
        Set<String> reservedCollisions = dataColumnNames.stream()
                .filter(name -> RESERVED_CHANGE_COLUMNS.contains(name.toLowerCase(ENGLISH)))
                .collect(toImmutableSet());
        if (!reservedCollisions.isEmpty()) {
            throw new TrinoException(
                    INVALID_PROCEDURE_ARGUMENT,
                    format("Source table contains columns with reserved names: %s", reservedCollisions));
        }
    }

    private void verifyColumnsAreComparable(Schema endSchema)
    {
        // The generated view collapses carryover rows by grouping on the full row value, so every data column must be
        // comparable. Every Iceberg type currently maps to a comparable Trino type, so this cannot fail today; the
        // check guards against a future non-comparable type becoming representable, turning what would otherwise be a
        // cryptic GROUP BY failure at query time into an actionable error here.
        List<String> nonComparableColumns = endSchema.columns().stream()
                .filter(field -> !toTrinoType(field.type(), typeManager).isComparable())
                .map(NestedField::name)
                .collect(toImmutableList());
        verify(nonComparableColumns.isEmpty(), "create_changelog_view: source columns are not comparable: %s", nonComparableColumns);
    }

    private static long requiredLongProperty(Map<String, Object> properties, String name)
    {
        Long value = (Long) properties.get(name);
        if (value == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, name + " is required");
        }
        return value;
    }

    private static void checkCopyOnWrite(Map<String, String> properties, String property, String defaultValue)
    {
        String value = properties.getOrDefault(property, defaultValue);
        if (!"copy-on-write".equals(value)) {
            throw new TrinoException(
                    INVALID_PROCEDURE_ARGUMENT,
                    format("create_changelog_view requires a copy-on-write table; property '%s' is set to '%s'.", property, value));
        }
    }

    private List<String> resolveIdentifierColumns(Map<String, Object> properties, Schema endSchema)
    {
        @SuppressWarnings("unchecked")
        List<String> requestedIdentifierColumns = (List<String>) properties.get("identifier_columns");
        if (requestedIdentifierColumns != null && !requestedIdentifierColumns.isEmpty()) {
            Set<String> resolved = new LinkedHashSet<>();
            for (String column : requestedIdentifierColumns) {
                if (column == null || column.isEmpty()) {
                    throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "identifier_columns entries cannot be null or empty");
                }
                if (RESERVED_CHANGE_COLUMNS.contains(column.toLowerCase(ENGLISH))) {
                    throw new TrinoException(
                            INVALID_PROCEDURE_ARGUMENT,
                            format("Identifier column '%s' uses a reserved column name", column));
                }
                NestedField field = endSchema.findField(column);
                if (field == null) {
                    throw new TrinoException(
                            INVALID_PROCEDURE_ARGUMENT,
                            format("Identifier column '%s' not found in table schema at end snapshot", column));
                }
                try {
                    IcebergSchemaUtils.validateIdentifierField(endSchema, field.fieldId());
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(
                            INVALID_PROCEDURE_ARGUMENT,
                            format("Identifier column '%s': %s", column, e.getMessage()),
                            e);
                }
                resolved.add(column);
            }
            return ImmutableList.copyOf(resolved);
        }

        // Schema-declared identifier fields were already validated by Iceberg when the Schema was constructed.
        Set<Integer> identifierFieldIds = endSchema.identifierFieldIds();
        if (!identifierFieldIds.isEmpty()) {
            ImmutableList.Builder<String> fromSchema = ImmutableList.builder();
            // Sort by field id so the generated view SQL (PARTITION BY column order) is reproducible.
            for (int fieldId : ImmutableList.sortedCopyOf(identifierFieldIds)) {
                String name = endSchema.findColumnName(fieldId);
                if (name == null) {
                    continue;
                }
                fromSchema.add(name);
            }
            List<String> result = fromSchema.build();
            if (!result.isEmpty()) {
                return result;
            }
        }

        throw new TrinoException(
                INVALID_PROCEDURE_ARGUMENT,
                "No identifier_columns provided and table has no identifier field IDs in its schema");
    }

    private List<ViewColumn> buildViewColumns(Schema endSchema)
    {
        ImmutableList.Builder<ViewColumn> columns = ImmutableList.builder();
        for (NestedField field : endSchema.columns()) {
            columns.add(new ViewColumn(field.name(), toTrinoType(field.type(), typeManager).getTypeId(), Optional.empty()));
        }
        columns.add(new ViewColumn("_change_version_id", BIGINT.getTypeId(), Optional.empty()));
        columns.add(new ViewColumn("_change_timestamp", TIMESTAMP_TZ_MILLIS.getTypeId(), Optional.empty()));
        columns.add(new ViewColumn("_change_ordinal", INTEGER.getTypeId(), Optional.empty()));
        columns.add(new ViewColumn("_change_type", VARCHAR.getTypeId(), Optional.empty()));
        return columns.build();
    }

    // system.table_changes is intentionally unqualified — the analyzer resolves it against the view's stored ConnectorViewDefinition catalog, not the calling session's catalog.
    private static final String VIEW_SQL_TEMPLATE =
            """
            WITH "$temp" AS (
                SELECT *
                FROM TABLE("system"."table_changes"(
                    schema_name => '%1$s',
                    table_name => '%2$s',
                    start_snapshot_id => %3$d,
                    end_snapshot_id => %4$d))
            ),
            "$collapsed" AS (
                SELECT %5$s,
                       "_change_version_id",
                       "_change_timestamp",
                       "_change_ordinal",
                       SUM(CASE WHEN "_change_type" = '%7$s' THEN 1 ELSE -1 END) AS "$change_value"
                FROM "$temp"
                GROUP BY %5$s,
                         "_change_version_id",
                         "_change_timestamp",
                         "_change_ordinal"
                HAVING SUM(CASE WHEN "_change_type" = '%7$s' THEN 1 ELSE -1 END) <> 0
            ),
            "$ranked" AS (
                SELECT *,
                       COUNT(*) OVER (PARTITION BY %6$s, "_change_version_id", "_change_ordinal") AS "$row_count"
                FROM "$collapsed"
            )
            SELECT %5$s,
                   "_change_version_id",
                   "_change_timestamp",
                   "_change_ordinal",
                   CASE
                       WHEN "$row_count" = 1 AND "$change_value" =  1 THEN '%7$s'
                       WHEN "$row_count" = 1 AND "$change_value" = -1 THEN '%8$s'
                       WHEN "$row_count" = 2 AND "$change_value" =  1 THEN '%9$s'
                       WHEN "$row_count" = 2 AND "$change_value" = -1 THEN '%10$s'
                       ELSE CAST(fail('Identifier columns do not uniquely identify a row') AS varchar)
                   END AS "_change_type"
            FROM "$ranked"\
            """;

    private static String buildViewSql(
            String schemaName,
            String tableName,
            long startSnapshotId,
            long endSnapshotId,
            List<NestedField> dataColumns,
            List<String> identifierColumns)
    {
        if (dataColumns.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Source table has no columns");
        }
        String dataColList = dataColumns.stream()
                .map(NestedField::name)
                .map(PartitionFields::quotedName)
                .collect(joining(", "));

        String idColList = identifierColumns.stream()
                .map(CreateChangelogView::toIdentifierExpression)
                .collect(joining(", "));

        return VIEW_SQL_TEMPLATE.formatted(
                escapeStringLiteral(schemaName),
                escapeStringLiteral(tableName),
                startSnapshotId,
                endSnapshotId,
                dataColList,
                idColList,
                escapeStringLiteral(INSERT_CHANGE_TYPE_VALUE),
                escapeStringLiteral(DELETE_CHANGE_TYPE_VALUE),
                escapeStringLiteral(UPDATE_AFTER_CHANGE_TYPE_VALUE),
                escapeStringLiteral(UPDATE_BEFORE_CHANGE_TYPE_VALUE));
    }

    private static String toIdentifierExpression(String dottedName)
    {
        return Splitter.on('.').splitToList(dottedName).stream()
                .map(PartitionFields::quotedName)
                .collect(joining("."));
    }

    private static String escapeStringLiteral(String value)
    {
        return value.replace("'", "''");
    }
}
