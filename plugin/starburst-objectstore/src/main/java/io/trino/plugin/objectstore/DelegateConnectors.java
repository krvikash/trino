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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.Connector;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record DelegateConnectors(
        @ForHive Connector hiveConnector,
        @ForIceberg Connector icebergConnector,
        @ForDelta Connector deltaConnector,
        @ForHudi Connector hudiConnector)
{
    @Inject
    public DelegateConnectors
    {
        requireNonNull(hiveConnector, "hiveConnector is null");
        requireNonNull(icebergConnector, "icebergConnector is null");
        requireNonNull(deltaConnector, "deltaConnector is null");
        requireNonNull(hudiConnector, "hudiConnector is null");
    }

    /**
     * Delegate connectors in canonical order of Hive, Iceberg, Delta, Hudi.
     */
    public List<Connector> asList()
    {
        return ImmutableList.of(hiveConnector, icebergConnector, deltaConnector, hudiConnector);
    }

    public Map<TableType, Connector> byType()
    {
        return ImmutableMap.<TableType, Connector>builder()
                .put(TableType.HIVE, hiveConnector)
                .put(TableType.ICEBERG, icebergConnector)
                .put(TableType.DELTA, deltaConnector)
                .put(TableType.HUDI, hudiConnector)
                .buildOrThrow();
    }
}
