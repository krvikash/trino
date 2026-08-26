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
import com.starburstdata.trino.plugin.functions.crypto.CryptoConfig;
import com.starburstdata.trino.plugin.functions.crypto.CryptoFunctions;
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
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.functions.ai.AiSessionPropertiesProvider.isBatchCallingEnabled;
import static com.starburstdata.trino.plugin.functions.crypto.CryptoFunctions.CRYPTO_SCHEMA_NAME;
import static io.trino.spi.function.FunctionKind.BATCH;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

public class FunctionsMetadata
        implements ConnectorMetadata
{
    public static final String AI_SCHEMA_NAME = "ai";
    public static final String IO_SCHEMA_NAME = "io";

    private final List<FunctionMetadata> aiFunctions;
    private final boolean cryptoEnabled;
    private final List<FunctionMetadata> cryptoFunctions;

    // TODO Bind the functions per schema, with a MapBinder keyed by schema name that each module
    //  contributes to, instead of the AI functions arriving through the unqualified
    //  List<FunctionMetadata> binding while the crypto ones are read statically. listSchemaNames()
    //  and functionsIn() would then derive from one source of truth instead of two that have to be
    //  kept in step. Deferred because it changes AiModule's bindings, which the tests reachable
    //  outside the 'ai' CI label do not cover. Can be done before GA of crypto functions.
    @Inject
    public FunctionsMetadata(List<FunctionMetadata> aiFunctions, CryptoConfig cryptoConfig)
    {
        this.aiFunctions = ImmutableList.copyOf(requireNonNull(aiFunctions, "aiFunctions is null"));
        this.cryptoEnabled = requireNonNull(cryptoConfig, "cryptoConfig is null").isEnabled();
        this.cryptoFunctions = cryptoEnabled ? CryptoFunctions.getFunctions() : ImmutableList.of();
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return functionsIn(schemaName);
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        boolean batchCallingEnabled = isBatchCallingEnabled(session);
        Map<Signature, List<FunctionMetadata>> candidates = functionsIn(name.schemaName()).stream()
                .filter(function -> function.getCanonicalName().equals(name.functionName()))
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
        return Stream.concat(aiFunctions.stream(), cryptoFunctions.stream())
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow();
    }

    /**
     * Functions are addressable only through the schema that declares them, so that
     * {@code starburst.crypto.aes_encrypt} resolves while {@code starburst.ai.aes_encrypt} does not.
     */
    private List<FunctionMetadata> functionsIn(String schemaName)
    {
        return switch (schemaName) {
            case AI_SCHEMA_NAME -> aiFunctions;
            case CRYPTO_SCHEMA_NAME -> cryptoFunctions;
            default -> ImmutableList.of();
        };
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableList.Builder<String> schemaNames = ImmutableList.<String>builder()
                .add(AI_SCHEMA_NAME)
                .add(IO_SCHEMA_NAME);
        if (cryptoEnabled) {
            schemaNames.add(CRYPTO_SCHEMA_NAME);
        }
        return schemaNames.build();
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
