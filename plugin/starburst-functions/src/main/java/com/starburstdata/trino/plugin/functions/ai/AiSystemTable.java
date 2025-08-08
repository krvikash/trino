/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import io.airlift.slice.Slices;
import io.starburst.ai.model.ConnectionInfo;
import io.starburst.ai.model.ModelConnectionSpec;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.ai.model.ConnectionInfo.AwsBedrockConnectionInfo;
import static io.starburst.ai.model.ConnectionInfo.OpenAiConnectionInfo;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public abstract class AiSystemTable<T extends ModelConnectionSpec>
        implements SystemTable
{
    private final ConnectorTableMetadata metadata;

    public AiSystemTable(ConnectorTableMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    protected abstract List<?> toRow(T spec);

    protected abstract Collection<T> getSpecs();

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return metadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<Type> types = metadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        List<List<?>> records = getSpecs().stream()
                .map(this::toRow)
                .collect(toImmutableList());
        return new InMemoryRecordSet(types, records).cursor();
    }

    protected static String getEndpoint(ModelConnectionSpec spec)
    {
        return switch (spec.connectionInfo()) {
            case OpenAiConnectionInfo openAiConnectionInfo -> openAiConnectionInfo.endpoint().orElse(null);
            case AwsBedrockConnectionInfo _ -> null;
        };
    }

    protected static Integer convertFloat(Optional<Float> value)
    {
        return value.map(Float::floatToIntBits).orElse(null);
    }

    protected static Block convertList(List<String> elements)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, elements.size());
        for (String element : elements) {
            VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(element));
        }
        return blockBuilder.build();
    }

    protected static String convertProvider(ConnectionInfo connectionInfo)
    {
        return switch (connectionInfo) {
            case OpenAiConnectionInfo _ -> "OPENAI";
            case AwsBedrockConnectionInfo _ -> "AWS_BEDROCK";
        };
    }
}
