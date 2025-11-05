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
package io.trino.plugin.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchTableHandle.Type.AGGREGATION;
import static io.trino.plugin.opensearch.OpenSearchUtils.isScrollable;
import static java.util.Objects.requireNonNull;

public class OpenSearchSplitManager
        implements ConnectorSplitManager
{
    private final OpenSearchClient client;
    // TODO remove this flag https://starburstdata.atlassian.net/browse/SEP-19965
    private final boolean scrollableRawQueryEnabled;
    private final boolean shardedScrollableRawQueryEnabled;
    private final ObjectMapper objectMapper;

    @Inject
    public OpenSearchSplitManager(OpenSearchClient client, OpenSearchConfig config, ObjectMapper objectMapper)
    {
        this.client = requireNonNull(client, "client is null");
        this.scrollableRawQueryEnabled = config.isScrollableRawQueryEnabled();
        this.shardedScrollableRawQueryEnabled = config.isShardedScrollableRawQueryEnabled();
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        OpenSearchTableHandle tableHandle = (OpenSearchTableHandle) table;

        if (shouldUseSingleSplit(tableHandle)) {
            return new FixedSplitSource(new OpenSearchSplit(tableHandle.index(), 0, Optional.empty()));
        }
        List<OpenSearchSplit> splits = client.getSearchShards(tableHandle.index()).stream()
                .map(shard -> new OpenSearchSplit(shard.index(), shard.id(), shard.address()))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }

    private boolean shouldUseSingleSplit(OpenSearchTableHandle tableHandle)
    {
        return client.isServerlessDeployment() ||
                tableHandle.type().equals(AGGREGATION) ||
                (tableHandle.type().equals(OpenSearchTableHandle.Type.QUERY) &&
                        (!scrollableRawQueryEnabled || !shardedScrollableRawQueryEnabled || !isScrollable(objectMapper, tableHandle.query().orElseThrow())));
    }
}
