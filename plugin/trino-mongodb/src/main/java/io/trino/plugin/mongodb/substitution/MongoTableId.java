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
package io.trino.plugin.mongodb.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class MongoTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("MongoTableId", 1);

    private final SchemaTableName schemaTableName;
    private final long hash;

    @JsonCreator
    public MongoTableId(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String schemaTableName)
    {
        this(new SchemaTableName(schemaName, schemaTableName));
    }

    public MongoTableId(SchemaTableName schemaTableName)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.hash = XxHash64.hash(schemaTableName.toString().getBytes(UTF_8));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaTableName.getSchemaName();
    }

    @JsonProperty
    public String getTableName()
    {
        return schemaTableName.getTableName();
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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
        if (!(o instanceof MongoTableId that)) {
            return false;
        }
        return schemaTableName.equals(that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return schemaTableName.hashCode();
    }
}
