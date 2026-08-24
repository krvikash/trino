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
package com.starburstdata.plugin.kdb.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class KdbTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("KdbTableId", 1);

    private final SchemaTableName tableName;
    private final long hash;

    @JsonCreator
    public KdbTableId(@JsonProperty("tableName") SchemaTableName tableName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.hash = XxHash64.hash(tableName.toString().getBytes(UTF_8));
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
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
        if (!(o instanceof KdbTableId that)) {
            return false;
        }
        return tableName.equals(that.tableName);
    }

    @Override
    public int hashCode()
    {
        return tableName.hashCode();
    }
}
