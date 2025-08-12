/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.io.functions.Load;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.functions.AiSessionPropertiesProvider.isBatchCallingEnabled;
import static io.trino.spi.function.FunctionKind.BATCH;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

public class FunctionsMetadata
        implements ConnectorMetadata
{
    public static final String AI_SCHEMA_NAME = "ai";
    public static final String IO_SCHEMA_NAME = "io";

    private final List<FunctionMetadata> functions;

    @Inject
    public FunctionsMetadata(List<FunctionMetadata> functions)
    {
        this.functions = ImmutableList.copyOf(requireNonNull(functions, "functions is null"));
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return schemaName.equals(AI_SCHEMA_NAME) ? functions : List.of();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        if (!listSchemaNames(session).contains(name.getSchemaName())) {
            return ImmutableList.of();
        }
        boolean batchCallingEnabled = isBatchCallingEnabled(session);
        Map<Signature, List<FunctionMetadata>> candidates = functions.stream()
                .filter(function -> function.getCanonicalName().equals(name.getFunctionName()))
                .collect(groupingBy(FunctionMetadata::getSignature));
        ImmutableList.Builder<FunctionMetadata> builder = ImmutableList.builder();
        for (List<FunctionMetadata> functions : candidates.values()) {
            if (functions.size() == 1) {
                builder.add(functions.getFirst());
            }
            else {
                // pick the one that matches the batch calling preference
                for (FunctionMetadata function : functions) {
                    if (batchCallingEnabled && function.getKind() == BATCH) {
                        builder.add(function);
                    }
                    else if (!batchCallingEnabled && function.getKind() == SCALAR) {
                        builder.add(function);
                    }
                }
            }
        }
        return builder.build();
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return functions.stream()
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow();
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(AI_SCHEMA_NAME, IO_SCHEMA_NAME);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof Load.LoadTableHandle loadTableHandle) {
            return new ConnectorTableMetadata(
                    // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                    new SchemaTableName("_generated", "_generated_load"),
                    loadTableHandle.columns().stream()
                            .map(column -> new ColumnMetadata(column.getName(), column.getType()))
                            .collect(toImmutableList()));
        }
        throw new IllegalArgumentException("Unsupported table handle: " + tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return column.getColumnMetadata();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof Load.LoadFunctionHandle loadFunctionHandle) {
            Load.LoadTableHandle tableHandle = loadFunctionHandle.tableHandle();
            List<ColumnHandle> columnHandles = tableHandle.columns().stream()
                    .map(ColumnHandle.class::cast)
                    .collect(toImmutableList());
            return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
        }
        return Optional.empty();
    }
}
