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
package io.trino.plugin.iceberg.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Objects;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class IcebergTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("IcebergTableId", 1);

    private final String schemaName;
    private final String tableName;
    private final UUID tableUuid;
    private final long hash;

    @JsonCreator
    public IcebergTableId(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableUuid") UUID tableUuid)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableUuid = requireNonNull(tableUuid, "tableUuid is null");
        XxHash64 hash = new XxHash64()
                .update(schemaName.getBytes(UTF_8))
                .update(tableName.getBytes(UTF_8))
                .update(tableUuid.toString().getBytes(UTF_8));
        this.hash = hash.hash();
    }

    @JsonProperty
    public String schemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String tableName()
    {
        return tableName;
    }

    @JsonProperty
    public UUID tableUuid()
    {
        return tableUuid;
    }

    @JsonIgnore
    @Override
    public long hash()
    {
        return hash;
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergTableId that)) {
            return false;
        }
        return schemaName.equals(that.schemaName)
                && tableName.equals(that.tableName)
                && tableUuid.equals(that.tableUuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableUuid);
    }
}
