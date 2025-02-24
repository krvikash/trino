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

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.GENERATE_EMBEDDINGS;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;

public class IcebergGenerateEmbeddingsProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                GENERATE_EMBEDDINGS.name(),
                distributedWithFilteringAndRepartitioning(),
                ImmutableList.of(
                        PropertyMetadata.stringProperty(
                                "embedding_column",
                                "Name of the column to store embeddings in.",
                                null,
                                false),
                        PropertyMetadata.stringProperty(
                                "data_column",
                                "Name of the column containing the data to use when creating the embeddings.",
                                null,
                                false),
                        PropertyMetadata.stringProperty(
                                "model_id",
                                "Identifier for the model to use for embeddings",
                                null,
                                false)));
    }
}
