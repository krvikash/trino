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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.starburst.materialization.HashUtil;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.substitution.ConnectorTableId;

import java.util.Objects;

import static io.starburst.materialization.HashUtil.combineHash;
import static java.util.Objects.requireNonNull;

public final class TableId
{
    private final CatalogName catalogName;
    private final ConnectorTableId connectorId;
    private final long hash;

    @JsonCreator
    public TableId(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("connectorId") ConnectorTableId connectorId)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.hash = combineHash(connectorId.hash(), HashUtil.hash(catalogName.toString()));
    }

    @JsonProperty
    public CatalogName catalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public ConnectorTableId connectorId()
    {
        return connectorId;
    }

    @JsonIgnore
    public long hash()
    {
        return hash;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableId that)) {
            return false;
        }
        return catalogName.equals(that.catalogName)
                && connectorId.equals(that.connectorId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, connectorId);
    }

    @Override
    public String toString()
    {
        return catalogName + ":" + connectorId;
    }
}
