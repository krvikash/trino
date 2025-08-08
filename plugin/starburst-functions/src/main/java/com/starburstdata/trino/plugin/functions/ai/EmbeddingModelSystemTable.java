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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.starburst.ai.client.ModelConnectionSpecDao;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.starburstdata.trino.plugin.functions.ai.AiMetadata.SCHEMA_NAME;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class EmbeddingModelSystemTable
        extends AiSystemTable<EmbeddingModelConnectionSpec>
{
    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName(SCHEMA_NAME, "embedding_models"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("id", VARCHAR))
                    .add(new ColumnMetadata("provider", VARCHAR))
                    .add(new ColumnMetadata("name", VARCHAR))
                    .add(new ColumnMetadata("inference_profile", VARCHAR))
                    .add(new ColumnMetadata("endpoint", VARCHAR))
                    .add(new ColumnMetadata("dimensions", INTEGER))
                    .build());

    private final ModelConnectionSpecDao dao;

    @Inject
    public EmbeddingModelSystemTable(ModelConnectionSpecDao dao)
    {
        super(METADATA);
        this.dao = requireNonNull(dao, "dao is null");
    }

    @Override
    protected List<?> toRow(EmbeddingModelConnectionSpec spec)
    {
        List<Object> row = new ArrayList<>();
        row.add(spec.id());
        row.add(convertProvider(spec.connectionInfo()));
        row.add(spec.modelName());
        row.add(spec.inferenceProfile().orElse(null));
        row.add(getEndpoint(spec));
        row.add(spec.dimensions().orElse(null));
        return row;
    }

    @Override
    protected Collection<EmbeddingModelConnectionSpec> getSpecs()
    {
        return dao.embeddingModelConnectionSpecs();
    }
}
