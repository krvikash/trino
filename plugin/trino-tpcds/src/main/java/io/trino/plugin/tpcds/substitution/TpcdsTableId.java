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
package io.trino.plugin.tpcds.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TpcdsTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("TpcdsTableId", 1);
    private static final byte[] PATH_SEPARATOR = {0};

    private final String tableName;
    private final double scaleFactor;
    private final long hash;

    @JsonCreator
    public TpcdsTableId(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scaleFactor") double scaleFactor)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.scaleFactor = scaleFactor;
        this.hash = new XxHash64()
                .update(tableName.getBytes(UTF_8))
                .update(PATH_SEPARATOR)
                .update(ByteBuffer.allocate(Double.BYTES).putDouble(scaleFactor).array())
                .hash();
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public double getScaleFactor()
    {
        return scaleFactor;
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
        if (!(o instanceof TpcdsTableId that)) {
            return false;
        }
        return Double.compare(scaleFactor, that.scaleFactor) == 0 && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, scaleFactor);
    }
}
