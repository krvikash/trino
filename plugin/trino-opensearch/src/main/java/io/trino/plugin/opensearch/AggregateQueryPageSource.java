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
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.TypeManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.Stats;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.updateCompositeAfterKey;
import static java.util.Objects.requireNonNull;

public class AggregateQueryPageSource
        implements ConnectorPageSource
{
    private static final SearchHit NO_HIT = new SearchHit(0);
    private final List<Decoder> decoders;

    private final OpenSearchClient client;
    private final OpenSearchTableHandle table;
    private final OpenSearchSplit split;
    private final BlockBuilder[] columnBuilders;
    private final List<OpenSearchColumnHandle> columns;
    private final QueryBuilder queryBuilder;
    private final OptionalInt pageSize;
    private final List<AggregationBuilder> baseAggregations;

    private long totalBytes;
    private long readTimeNanos;
    private Optional<Map<String, Object>> after = Optional.empty();
    private boolean fetched;
    private long fetchedSize;

    public AggregateQueryPageSource(
            OpenSearchClient client,
            TypeManager typeManager,
            OpenSearchTableHandle table,
            OpenSearchSplit split,
            List<OpenSearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columns, "columns is null");

        this.client = client;
        this.table = table;
        this.split = split;
        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(columns);

        columnBuilders = columns.stream()
                .map(OpenSearchColumnHandle::type)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
        this.queryBuilder = buildSearchQuery(table.constraint().transformKeys(OpenSearchColumnHandle.class::cast), table.query(), table.regexes());
        // set pageSize to search.max_buckets https://docs.opensearch.org/docs/latest/install-and-configure/configuring-opensearch/search-settings/ or the limit if it is smaller
        this.pageSize = table.limit().isEmpty()
                ? OptionalInt.empty()
                : OptionalInt.of((int) Math.min(Math.min(table.limit().getAsLong(), Integer.MAX_VALUE), client.getMaxAggregationBuckets()));

        this.baseAggregations = buildAggregationQuery(table.aggregationInfo().get(), pageSize);
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        // One of the following situation may stop the fetching
        // 1. afterKey is empty, that means no more result can be fetched
        // 2. fetchedSize >= the potential limit constraint
        return (fetched && after.isEmpty()) || (table.limit().isPresent() && fetchedSize >= table.limit().getAsLong());
    }

    @Override
    @SuppressWarnings("deprecation") // TODO (https://github.com/trinodb/trino/issues/29959) migrate to MemoryContext
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}

    @Override
    public SourcePage getNextSourcePage()
    {
        long start = System.nanoTime();

        List<AggregationBuilder> aggregations = updateCompositeAfterKey(baseAggregations, after);

        SearchResponse searchResponse = client.beginSearch(
                split.index(),
                split.shard(),
                this.queryBuilder,
                Optional.empty(),
                ImmutableList.of(),
                Optional.of(aggregations),
                Optional.empty(),
                table.limit());
        readTimeNanos += System.nanoTime() - start;
        fetched = true;
        AggregationResult aggregationResult = processSearchResponse(searchResponse);
        List<Map<String, Object>> flatResult = aggregationResult.results();
        fetchedSize += flatResult.size();
        after = aggregationResult.afterKey();
        for (Map<String, Object> result : flatResult) {
            for (int i = 0; i < columns.size(); i++) {
                String key = columns.get(i).name();
                decoders.get(i).decode(NO_HIT, () -> result.get(key), columnBuilders[i]);
            }
        }

        // estimate bytes, since each aggregated value is numeric (8 bytes)
        // TODO: estimate bytes more accurately based on keys types as well
        totalBytes += (long) flatResult.size() * columns.size() * 8;
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        return SourcePage.create(new Page(blocks));
    }

    private AggregationResult processSearchResponse(SearchResponse searchResponse)
    {
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return new AggregationResult(ImmutableList.of(), Optional.empty());
        }

        verifyAggregationTypes(aggregations);

        boolean hasBucketsAggregation = aggregations.asList().stream()
                .anyMatch(agg -> agg instanceof CompositeAggregation);

        ImmutableList.Builder<Map<String, Object>> groupedResults = ImmutableList.builder();
        Map<String, Object> flatResultMap = new LinkedHashMap<>();
        Optional<Map<String, Object>> afterKey = Optional.empty();

        for (Aggregation aggregation : aggregations) {
            switch (aggregation) {
                case CompositeAggregation composite -> {
                    for (CompositeAggregation.Bucket bucket : composite.getBuckets()) {
                        Map<String, Object> row = new LinkedHashMap<>(bucket.getKey());
                        for (Aggregation metricAgg : bucket.getAggregations()) {
                            switch (metricAgg) {
                                case NumericMetricsAggregation.SingleValue singleValue -> row.put(metricAgg.getName(), extractSingleValue(singleValue));
                                case Stats statsAgg -> row.put(metricAgg.getName(), extractSumFromStatsValue(statsAgg));
                                case null, default -> throw new IllegalStateException("Unsupported metric aggregation in bucket: " + metricAgg);
                            }
                        }
                        groupedResults.add(row);
                    }

                    afterKey = Optional.ofNullable(composite.afterKey());
                }

                case NumericMetricsAggregation.SingleValue singleValue -> flatResultMap.put(aggregation.getName(), extractSingleValue(singleValue));
                case Stats statsAgg -> flatResultMap.put(aggregation.getName(), extractSumFromStatsValue(statsAgg));
                case null, default -> throw new IllegalStateException("Unrecognized aggregation type: " + (aggregation == null ? "null" : aggregation.getType()));
            }
        }

        if (hasBucketsAggregation) {
            return new AggregationResult(groupedResults.build(), afterKey);
        }
        return new AggregationResult(ImmutableList.of(flatResultMap), Optional.empty());
    }

    private void verifyAggregationTypes(Aggregations aggregations)
    {
        boolean hasComposite = false;
        boolean hasMetric = false;

        for (Aggregation aggregation : aggregations) {
            switch (aggregation) {
                case CompositeAggregation _ -> hasComposite = true;
                case NumericMetricsAggregation.SingleValue _, Stats _ -> hasMetric = true;
                case null, default -> throw new IllegalStateException(
                        "Unrecognized aggregation type: " + (aggregation == null ? "null" : aggregation.getType()));
            }
        }

        if (hasComposite && hasMetric) {
            throw new IllegalStateException("Cannot mix bucket and metric aggregations in the same query.");
        }
    }

    private Double extractSumFromStatsValue(Stats statsValue)
    {
        // Check min(not avg) result to see if the sum result is valid.
        // OS server return null for avg/min/max, but the os client return 0 as the result of avg even server return null.
        // assumption as per ES code org.elasticsearch.search.aggregations.metrics.stats.ParsedStats
        if (Double.isInfinite(statsValue.getMin())) {
            return null;
        }
        return statsValue.getSum();
    }

    private Double extractSingleValue(NumericMetricsAggregation.SingleValue singleValue)
    {
        // null will be decoded as Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
        double value = singleValue.value();
        if (Double.isInfinite(value)) {
            return null;
        }
        return value;
    }

    private List<Decoder> createDecoders(List<OpenSearchColumnHandle> columns)
    {
        return columns.stream()
                .map(OpenSearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
    }

    private record AggregationResult(List<Map<String, Object>> results, Optional<Map<String, Object>> afterKey)
    {
        private AggregationResult
        {
            requireNonNull(results, "results is null");
            requireNonNull(afterKey, "afterKey is null");
        }
    }
}
