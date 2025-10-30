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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ScrollablePassthroughQueryPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(PassthroughQueryPageSource.class);

    private final Set<String> seenScrollIds = new HashSet<>();
    private final OpenSearchClient client;
    private final String index;
    private final String query;
    private long readTimeNanos;
    private String scrollId;
    private long completedBytes;
    private long hitsSoFar;
    private boolean done;

    public ScrollablePassthroughQueryPageSource(OpenSearchClient client, OpenSearchTableHandle table)
    {
        requireNonNull(table, "table is null");
        this.client = requireNonNull(client, "client is null");
        this.index = table.index();
        this.query = table.query().orElseThrow();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (done) {
            return null;
        }
        SearchResponse searchResponse;
        long start = System.nanoTime();
        if (scrollId == null) {
            searchResponse = client.executeInitialScrollableQuery(index, query);
        }
        else {
            searchResponse = client.nextPage(scrollId);
        }
        readTimeNanos += System.nanoTime() - start;

        scrollId = searchResponse.getScrollId();
        SearchHits searchHits = searchResponse.getHits();
        checkArgument(searchHits.getTotalHits() != null, "Expected total hits to be non-null");
        long totalHits = searchHits.getTotalHits().value;
        seenScrollIds.add(scrollId);
        hitsSoFar += searchHits.getHits().length;

        if (hitsSoFar >= totalHits || searchHits.getHits().length == 0) {
            done = true;
            clearScrolls();
        }

        String result = searchResponse.toString();
        Slice slice = Slices.utf8Slice(result);
        BlockBuilder column = VARCHAR.createBlockBuilder(null, 1, result.length());
        VARCHAR.writeSlice(column, slice);
        SourcePage page = SourcePage.create(column.build());
        completedBytes += result.length();

        return page;
    }

    private void clearScrolls()
    {
        try {
            if (seenScrollIds.isEmpty()) {
                return;
            }
            client.clearScrolls(ImmutableList.copyOf(seenScrollIds));
        }
        catch (Exception e) {
            // ignore
            LOG.warn(e, "Error clearing scroll");
        }
        seenScrollIds.clear();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        clearScrolls();
    }
}
