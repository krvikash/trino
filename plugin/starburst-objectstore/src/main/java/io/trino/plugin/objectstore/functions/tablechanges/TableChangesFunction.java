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
package io.trino.plugin.objectstore.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.objectstore.ObjectStoreSessionProperties;
import io.trino.plugin.objectstore.ObjectStoreTransactionHandle;
import io.trino.plugin.objectstore.TableType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.plugin.objectstore.ObjectStoreMetadata.isError;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final String SCHEMA_NAME = "system";
    private static final String NAME = "table_changes";
    public static final String SCHEMA_NAME_ARGUMENT = "SCHEMA_NAME";
    private static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    private static final String SINCE_VERSION_ARGUMENT = "SINCE_VERSION";
    private static final String START_SNAPSHOT_VAR_NAME = "START_SNAPSHOT_ID";
    private static final String END_SNAPSHOT_VAR_NAME = "END_SNAPSHOT_ID";

    private final Connector icebergConnector;
    private final Connector deltaConnector;
    private final ConnectorTableFunction icebergTableChanges;
    private final ConnectorTableFunction deltaTableChanges;
    private final ObjectStoreSessionProperties sessionProperties;

    public TableChangesFunction(Connector icebergConnector, Connector deltaConnector, ObjectStoreSessionProperties sessionProperties)
    {
        super(
                SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(SINCE_VERSION_ARGUMENT).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(START_SNAPSHOT_VAR_NAME).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(END_SNAPSHOT_VAR_NAME).type(BIGINT).defaultValue(null).build()),
                GENERIC_TABLE);
        this.icebergConnector = requireNonNull(icebergConnector, "icebergConnector is null");
        this.deltaConnector = requireNonNull(deltaConnector, "deltaConnector is null");
        this.deltaTableChanges = deltaConnector
                .getTableFunctions().stream()
                .filter(connectorTableFunction -> connectorTableFunction.getName().equals("table_changes"))
                .collect(onlyElement());
        this.icebergTableChanges = icebergConnector
                .getTableFunctions().stream()
                .filter(connectorTableFunction -> connectorTableFunction.getName().equals("table_changes"))
                .collect(onlyElement());
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        ScalarArgument schemaNameArgument = (ScalarArgument) arguments.get(SCHEMA_NAME_ARGUMENT);
        checkFunctionArgument(schemaNameArgument.getValue() != null, "schema_name cannot be null");

        ScalarArgument tableNameArgument = (ScalarArgument) arguments.get(TABLE_NAME_ARGUMENT);
        checkFunctionArgument(tableNameArgument.getValue() != null, "table_name value for function table_changes() cannot be null");

        SchemaTableName schemaTableName = new SchemaTableName(((Slice) schemaNameArgument.getValue()).toStringUtf8(), ((Slice) tableNameArgument.getValue()).toStringUtf8());

        ScalarArgument sinceVersionArgument = (ScalarArgument) arguments.get(SINCE_VERSION_ARGUMENT);
        ScalarArgument startSnapshotArgument = (ScalarArgument) arguments.get(START_SNAPSHOT_VAR_NAME);
        ScalarArgument endSnapshotArgument = (ScalarArgument) arguments.get(END_SNAPSHOT_VAR_NAME);

        TableType tableType = getTableTypeInOrder(session, (ObjectStoreTransactionHandle) transaction, schemaTableName);
        switch (tableType) {
            case DELTA -> {
                checkFunctionArgument(startSnapshotArgument.getValue() == null, "unexpected argument: start_snapshot_id for table_changes() in DELTA connector");
                checkFunctionArgument(endSnapshotArgument.getValue() == null, "unexpected argument: end_snapshot_id for table_changes() in DELTA connector");
                return deltaTableChanges.analyze(
                        sessionProperties.unwrap(DELTA, session),
                        transaction,
                        ImmutableMap.of(
                                SCHEMA_NAME_ARGUMENT, schemaNameArgument,
                                TABLE_NAME_ARGUMENT, tableNameArgument,
                                SINCE_VERSION_ARGUMENT, sinceVersionArgument),
                        accessControl);
            }
            case ICEBERG -> {
                // The `since_version` argument is the third argument, if it is not null means user calls table_changes() with anonymous
                // arguments in Iceberg, where the third argument is `start_snapshot_id` and the fourth argument is `end_snapshot_id`
                // Otherwise, means user calls table_changes() with named arguments
                if (sinceVersionArgument.getValue() == null) {
                    checkFunctionArgument(startSnapshotArgument.getValue() != null, "start_snapshot_id must be provided for table_changes() in ICEBERG connector");
                    checkFunctionArgument(endSnapshotArgument.getValue() != null, "end_snapshot_id must be provided for table_changes() in ICEBERG connector");
                    return icebergTableChanges.analyze(
                            sessionProperties.unwrap(ICEBERG, session),
                            transaction,
                            ImmutableMap.of(
                                    SCHEMA_NAME_ARGUMENT, schemaNameArgument,
                                    TABLE_NAME_ARGUMENT, tableNameArgument,
                                    START_SNAPSHOT_VAR_NAME, startSnapshotArgument,
                                    END_SNAPSHOT_VAR_NAME, endSnapshotArgument),
                            accessControl);
                }
                else {
                    checkFunctionArgument(startSnapshotArgument.getValue() != null && endSnapshotArgument.getValue() == null, "Iceberg table_changes() function requires exactly 4 arguments");
                    return icebergTableChanges.analyze(
                            sessionProperties.unwrap(ICEBERG, session),
                            transaction,
                            ImmutableMap.of(
                                    SCHEMA_NAME_ARGUMENT, schemaNameArgument,
                                    TABLE_NAME_ARGUMENT, tableNameArgument,
                                    START_SNAPSHOT_VAR_NAME, sinceVersionArgument,
                                    END_SNAPSHOT_VAR_NAME, startSnapshotArgument),
                            accessControl);
                }
            }
            default -> throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Unsupported table type: " + tableType);
        }
    }

    private TableType getTableTypeInOrder(ConnectorSession session, ObjectStoreTransactionHandle handle, SchemaTableName schemaTableName)
    {
        ConnectorMetadata deltaMetadata = deltaConnector.getMetadata(session, handle.getDeltaHandle());
        if (canResolveTable(session, schemaTableName, deltaMetadata, DELTA)) {
            return DELTA;
        }

        ConnectorMetadata icebergMetadata = icebergConnector.getMetadata(session, handle.getIcebergHandle());
        if (canResolveTable(session, schemaTableName, icebergMetadata, ICEBERG)) {
            return ICEBERG;
        }

        throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "The connector is not supported for table_changes() function: " + schemaTableName);
    }

    private boolean canResolveTable(ConnectorSession session, SchemaTableName schemaTableName, ConnectorMetadata connectorMetadata, TableType tableType)
    {
        try {
            ConnectorTableHandle tableHandle = connectorMetadata.getTableHandle(sessionProperties.unwrap(tableType, session), schemaTableName, Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                return true;
            }
        }
        catch (TrinoException e) {
            if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                throw e;
            }
        }
        return false;
    }
}
