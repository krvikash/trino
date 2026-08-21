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
package io.trino.plugin.pinot.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class PinotTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("PinotTableId", 1);

    private static final byte[] PATH_SEPARATOR = {0};

    private final String schema;
    private final String table;
    private final long hash;

    @JsonCreator
    public PinotTableId(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.hash = new XxHash64()
                .update(schema.getBytes(UTF_8))
                .update(PATH_SEPARATOR)
                .update(table.getBytes(UTF_8))
                .hash();
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
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
        if (!(o instanceof PinotTableId that)) {
            return false;
        }
        return schema.equals(that.schema) && table.equals(that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table);
    }
}
