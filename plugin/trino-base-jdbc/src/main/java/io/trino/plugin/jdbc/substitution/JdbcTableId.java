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
package io.trino.plugin.jdbc.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class JdbcTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("JdbcTableId", 1);

    private final JdbcNamedRelationHandle relationHandle;
    private final long hash;

    @JsonCreator
    public JdbcTableId(@JsonProperty("relationHandle") JdbcNamedRelationHandle relationHandle)
    {
        this.relationHandle = requireNonNull(relationHandle, "relationHandle is null");
        this.hash = XxHash64.hash(relationHandle.getSchemaTableName().toString().getBytes(UTF_8));
    }

    @JsonProperty
    public JdbcNamedRelationHandle relationHandle()
    {
        return relationHandle;
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
        if (!(o instanceof JdbcTableId that)) {
            return false;
        }
        return relationHandle.equals(that.relationHandle);
    }

    @Override
    public int hashCode()
    {
        return relationHandle.hashCode();
    }
}
