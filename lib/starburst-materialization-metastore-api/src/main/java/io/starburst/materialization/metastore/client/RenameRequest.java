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
package io.starburst.materialization.metastore.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.connector.CatalogSchemaTableName;

import static java.util.Objects.requireNonNull;

public record RenameRequest(
        @JsonProperty CatalogSchemaTableName source,
        @JsonProperty CatalogSchemaTableName target,
        @JsonProperty StorageTableId targetStorageTableId)
{
    @JsonCreator
    public RenameRequest(
            @JsonProperty("source") CatalogSchemaTableName source,
            @JsonProperty("target") CatalogSchemaTableName target,
            @JsonProperty("targetStorageTableId") StorageTableId targetStorageTableId)
    {
        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.targetStorageTableId = requireNonNull(targetStorageTableId, "targetStorageTableId is null");
    }
}
