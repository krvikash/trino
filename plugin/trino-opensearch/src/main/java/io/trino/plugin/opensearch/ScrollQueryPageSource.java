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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.type.TypeManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.util.List;
import java.util.OptionalLong;
import java.util.function.Supplier;

public class ScrollQueryPageSource
        extends AbstractScanQueryPageSource
{
    private static final Logger LOG = Logger.get(ScrollQueryPageSource.class);

    public ScrollQueryPageSource(
            OpenSearchClient client,
            TypeManager typeManager,
            OpenSearchTableHandle table,
            OpenSearchSplit split,
            List<OpenSearchColumnHandle> columns)
    {
        super(client, typeManager, table, split, columns);
    }

    @Override
    protected TrackedSearchIterator createIterator(
            OpenSearchClient client,
            QueryBuilder query,
            OpenSearchSplit split,
            List<String> documentFields,
            OptionalLong limit,
            SearchResponse firstPage)
    {
        return new SearchHitIterator(client, () -> firstPage, limit);
    }

    private static class SearchHitIterator
            extends AbstractIterator<SearchHit>
            implements TrackedSearchIterator
    {
        private final OpenSearchClient client;
        private final Supplier<SearchResponse> first;
        private final OptionalLong limit;

        private SearchHits searchHits;
        private String scrollId;
        private int currentPosition;

        private long readTimeNanos;
        private long totalRecordCount;

        public SearchHitIterator(OpenSearchClient client, Supplier<SearchResponse> first, OptionalLong limit)
        {
            this.client = client;
            this.first = first;
            this.limit = limit;
            this.totalRecordCount = 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        protected SearchHit computeNext()
        {
            if (limit.isPresent() && totalRecordCount == limit.getAsLong()) {
                // No more record is necessary.
                return endOfData();
            }

            if (scrollId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == searchHits.getHits().length) {
                long start = System.nanoTime();
                SearchResponse response = client.nextPage(scrollId);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition);
            currentPosition++;
            totalRecordCount++;

            return hit;
        }

        private void reset(SearchResponse response)
        {
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }

        @Override
        public void close()
        {
            if (scrollId != null) {
                try {
                    client.clearScrolls(ImmutableList.of(scrollId));
                }
                catch (Exception e) {
                    // ignore
                    LOG.debug(e, "Error clearing scroll");
                }
            }
        }
    }
}
