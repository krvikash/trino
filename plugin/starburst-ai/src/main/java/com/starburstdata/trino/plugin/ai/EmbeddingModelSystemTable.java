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
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.starburst.ai.client.EmbeddingModelConnectionSpec;
import io.starburst.ai.client.ModelConnectionSpecDao;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.starburstdata.trino.plugin.ai.AiMetadata.SCHEMA_NAME;
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
