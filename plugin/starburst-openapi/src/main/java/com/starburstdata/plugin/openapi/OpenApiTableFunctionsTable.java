/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class OpenApiTableFunctionsTable
        implements SystemTable
{
    private final ConnectorTableMetadata metadata;
    private final OpenApiSpec spec;

    @Inject
    public OpenApiTableFunctionsTable(OpenApiSpec spec)
    {
        this.spec = requireNonNull(spec, "spec is null");
        this.metadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "table_functions"),
                ImmutableList.of(
                        new ColumnMetadata("function_name", VARCHAR),
                        new ColumnMetadata("api_path", VARCHAR),
                        new ColumnMetadata("description", VARCHAR),
                        new ColumnMetadata("input_columns", new ArrayType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("name"), VARCHAR),
                                new RowType.Field(Optional.of("type"), VARCHAR),
                                new RowType.Field(Optional.of("required"), BOOLEAN))))),
                        new ColumnMetadata("output_columns", new ArrayType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("name"), VARCHAR),
                                new RowType.Field(Optional.of("type"), VARCHAR)))))));
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<OpenApiSpec.TableFunctionDetail> details = spec.getTableFunctionDetails();
        int positionCount = details.size();

        Map<String, BlockBuilder> blockBuilders = metadata.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, column -> column.getType().createBlockBuilder(null, positionCount)));

        for (OpenApiSpec.TableFunctionDetail detail : details) {
            VARCHAR.writeString(blockBuilders.get("function_name"), detail.functionName());
            VARCHAR.writeString(blockBuilders.get("api_path"), detail.apiPath());

            if (detail.description().isPresent()) {
                VARCHAR.writeString(blockBuilders.get("description"), detail.description().get());
            }
            else {
                blockBuilders.get("description").appendNull();
            }

            writeInputColumns((ArrayBlockBuilder) blockBuilders.get("input_columns"), detail.inputParameters());
            writeOutputColumns((ArrayBlockBuilder) blockBuilders.get("output_columns"), detail.outputColumns());
        }

        Block[] blocks = metadata.getColumns().stream()
                .map(column -> blockBuilders.get(column.getName()).build())
                .toArray(Block[]::new);

        return new FixedPageSource(ImmutableList.of(new Page(positionCount, blocks)));
    }

    private static void writeInputColumns(ArrayBlockBuilder blockBuilder, Map<String, OpenApiParameterHandle> parameters)
    {
        if (parameters.isEmpty()) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> {
            for (Map.Entry<String, OpenApiParameterHandle> entry : parameters.entrySet()) {
                ((RowBlockBuilder) elementBuilder).buildEntry(fieldBuilders -> {
                    VARCHAR.writeString(fieldBuilders.get(0), entry.getKey().toLowerCase(ENGLISH));
                    VARCHAR.writeString(fieldBuilders.get(1), entry.getValue().getType().getDisplayName());
                    BOOLEAN.writeBoolean(fieldBuilders.get(2), entry.getValue().required());
                });
            }
        });
    }

    private static void writeOutputColumns(ArrayBlockBuilder blockBuilder, List<OpenApiColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            blockBuilder.appendNull();
            return;
        }
        blockBuilder.buildEntry(elementBuilder -> {
            for (OpenApiColumnHandle column : columns) {
                ((RowBlockBuilder) elementBuilder).buildEntry(fieldBuilders -> {
                    VARCHAR.writeString(fieldBuilders.get(0), column.name());
                    VARCHAR.writeString(fieldBuilders.get(1), column.type().getDisplayName());
                });
            }
        });
    }
}
