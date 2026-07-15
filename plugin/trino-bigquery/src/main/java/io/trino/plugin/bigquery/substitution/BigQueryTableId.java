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
package io.trino.plugin.bigquery.substitution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.XxHash64;
import io.trino.plugin.bigquery.RemoteTableName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Persisted identity of a BigQuery source table. A BigQuery table is uniquely identified by its
 * physical coordinates (project + dataset + table), captured here via {@link RemoteTableName}. Read
 * path and mutable-metadata attributes carried by {@code BigQueryNamedRelationHandle} — partitioning,
 * storage-API selection, comment — are intentionally excluded so they cannot break substitution
 * matching.
 */
public final class BigQueryTableId
        implements ConnectorTableId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("BigQueryTableId", 1);

    private final RemoteTableName remoteTableName;
    private final long hash;

    @JsonCreator
    public BigQueryTableId(@JsonProperty("remoteTableName") RemoteTableName remoteTableName)
    {
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.hash = XxHash64.hash(remoteTableName.toString().getBytes(UTF_8));
    }

    @JsonProperty
    public RemoteTableName remoteTableName()
    {
        return remoteTableName;
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
        if (!(o instanceof BigQueryTableId that)) {
            return false;
        }
        return remoteTableName.equals(that.remoteTableName);
    }

    @Override
    public int hashCode()
    {
        return remoteTableName.hashCode();
    }
}
