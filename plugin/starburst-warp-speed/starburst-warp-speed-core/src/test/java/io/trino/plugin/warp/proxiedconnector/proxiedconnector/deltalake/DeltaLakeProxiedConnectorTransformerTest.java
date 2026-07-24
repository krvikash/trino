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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.deltalake;

import io.trino.plugin.deltalake.DeltaLakeSplit;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.spi.SplitWeight;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class DeltaLakeProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final DeltaLakeProxiedConnectorTransformer deltaLakeProxiedConnectorTransformer =
            new DeltaLakeProxiedConnectorTransformer(new ProxiedConnectorConfig());

    @Test
    public void testCreateDispatcherSplit()
    {
        DeltaLakeSplit deltaLakeSplit = new DeltaLakeSplit(
                "path",
                1L,
                2L,
                3L,
                Optional.empty(),
                4L,
                Optional.empty(),
                Optional.empty(),
                SplitWeight.fromProportion(5L),
                TupleDomain.all(),
                Map.of("part1", Optional.of("part1")));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                getDeltaLakeTableHandle(),
                Optional.empty(),
                Metrics.EMPTY,
                false,
                Set.of());

        DispatcherSplit expectedDispatcherSplit = getDispatcherSplit(deltaLakeSplit, dispatcherTableHandle);

        super.testCreateDispatcherSplit(
                deltaLakeProxiedConnectorTransformer,
                deltaLakeSplit,
                dispatcherTableHandle,
                expectedDispatcherSplit);
    }

    private DispatcherSplit getDispatcherSplit(DeltaLakeSplit deltaLakeSplit, DispatcherTableHandle dispatcherTableHandle)
    {
        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (Map.Entry<String, Optional<String>> entry : deltaLakeSplit.partitionKeys().entrySet()) {
            if (entry.getValue().isPresent()) {
                partitionKeys.add(new PartitionKey(new RegularColumn(entry.getKey()), entry.getValue().orElseThrow()));
            }
        }
        return new DispatcherSplit(
                dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                deltaLakeSplit.path(),
                deltaLakeSplit.start(),
                deltaLakeSplit.length(),
                deltaLakeSplit.fileModifiedTime(),
                partitionKeys,
                "",
                deltaLakeSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        DeltaLakeTableHandle tableHandle = getDeltaLakeTableHandle();
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                tableHandle,
                Optional.empty(),
                Metrics.EMPTY,
                false,
                Set.of());

        super.testCreateProxyTableHandleForWarming(
                deltaLakeProxiedConnectorTransformer,
                dispatcherTableHandle,
                new DeltaLakeTableHandle(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        tableHandle.isManaged(),
                        tableHandle.getTableId(),
                        tableHandle.getLocation(),
                        tableHandle.getMetadataEntry(),
                        tableHandle.getProtocolEntry(),
                        TupleDomain.all(),
                        TupleDomain.all(),
                        tableHandle.isMerge(),
                        tableHandle.getProjectedColumns(),
                        tableHandle.getAnalyzeHandle(),
                        tableHandle.getReadVersion(),
                        tableHandle.isTimeTravel()));
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                getDeltaLakeTableHandle(),
                Optional.empty(),
                Metrics.EMPTY,
                false,
                Set.of());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                deltaLakeProxiedConnectorTransformer,
                dispatcherTableHandle,
                dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    private static DeltaLakeTableHandle getDeltaLakeTableHandle()
    {
        return new DeltaLakeTableHandle(
                "schemaName",
                "tableName",
                true,
                Optional.empty(),
                "location",
                mock(MetadataEntry.class),
                mock(ProtocolEntry.class),
                TupleDomain.all(),
                TupleDomain.all(),
                false,
                Optional.empty(),
                Optional.empty(),
                1L,
                false);
    }
}
