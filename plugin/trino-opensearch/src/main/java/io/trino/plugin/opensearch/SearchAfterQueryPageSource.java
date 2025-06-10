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
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.spi.type.TypeManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class SearchAfterQueryPageSource
        extends AbstractScanQueryPageSource
{
    private static final String SORT_FIELD = "_doc";

    public SearchAfterQueryPageSource(
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
        return new SearchAfterIterator(client, query, split.index(), () -> firstPage, documentFields, limit);
    }

    @Override
    protected Optional<String> defaultSort(OpenSearchTableHandle table)
    {
        // For search_after (reference https://www.elastic.co/guide/en/elasticsearch/reference/8.18/paginate-search-results.html#search-after)
        // We need to sort based on a unique field in the index, _doc gets special treatment in OpenSearch and is more efficient
        // TODO: Consider making the sort_field configurable
        return Optional.of(SORT_FIELD);
    }

    private static class SearchAfterIterator
            extends AbstractIterator<SearchHit>
            implements TrackedSearchIterator
    {
        private final OpenSearchClient client;
        private final Supplier<SearchResponse> firstPageSupplier;
        private final OptionalLong limit;
        private final QueryBuilder query;
        private final String index;
        private final List<String> documentFields;

        private SearchHits searchHits;
        private Object[] searchAfter;
        private int currentPosition;
        private boolean firstPageConsumed;

        private long readTimeNanos;
        private long totalRecordCount;

        public SearchAfterIterator(OpenSearchClient client, QueryBuilder query, String index, Supplier<SearchResponse> firstPageSupplier, List<String> documentFields, OptionalLong limit)
        {
            this.client = requireNonNull(client, "client is null");
            this.query = requireNonNull(query, "query is null");
            this.index = requireNonNull(index, "index is null");
            this.firstPageSupplier = requireNonNull(firstPageSupplier, "firstPageSupplier is null");
            this.documentFields = requireNonNull(documentFields, "documentFields is null");
            this.limit = requireNonNull(limit, "limit is null");
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
                return endOfData();
            }

            // If we have exhausted the current batch, fetch the next one using search_after
            if (searchHits == null || currentPosition == searchHits.getHits().length) {
                SearchResponse response;
                long start = System.nanoTime();
                if (!firstPageConsumed) {
                    response = firstPageSupplier.get();
                    firstPageConsumed = true;
                }
                else {
                    response = client.searchAfter(query, index, documentFields, limit, searchAfter, SORT_FIELD, totalRecordCount);
                }
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (searchHits == null || currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition++);
            totalRecordCount++;

            // Prepare search_after for the next batch
            if (currentPosition == searchHits.getHits().length && searchHits.getHits().length > 0) {
                searchAfter = searchHits.getHits()[searchHits.getHits().length - 1].getSortValues();
            }

            return hit;
        }

        private void reset(SearchResponse response)
        {
            // searchAfter is set in computeNext after the last hit is consumed
            searchHits = response.getHits();
            currentPosition = 0;
        }

        @Override
        public void close()
        {
            // No resources to clean up for search_after
        }
    }
}
