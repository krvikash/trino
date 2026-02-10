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
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.BuiltinColumns.SOURCE;
import static io.trino.plugin.opensearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.plugin.opensearch.OpenSearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

public abstract class AbstractScanQueryPageSource
        implements ConnectorPageSource
{
    private final List<Decoder> decoders;

    private final TrackedSearchIterator iterator;
    private final BlockBuilder[] columnBuilders;
    private final List<OpenSearchColumnHandle> columns;
    private long totalBytes;
    private long readTimeNanos;

    public AbstractScanQueryPageSource(
            OpenSearchClient client,
            TypeManager typeManager,
            OpenSearchTableHandle table,
            OpenSearchSplit split,
            List<OpenSearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);
        this.decoders = createDecoders(columns);

        // When the _source field is requested, we need to bypass column pruning when fetching the document
        boolean needAllFields = columns.stream()
                .map(OpenSearchColumnHandle::name)
                .anyMatch(isEqual(SOURCE.getName()));

        // Columns to fetch as doc_fields instead of pulling them out of the JSON source
        // This is convenient for types such as DATE, TIMESTAMP, etc, which have multiple possible
        // representations in JSON, but a single normalized representation as doc_field.
        List<String> documentFields = flattenFields(columns).entrySet().stream()
                .filter(entry -> entry.getValue().equals(TIMESTAMP_MILLIS))
                .map(Entry::getKey)
                .collect(toImmutableList());

        columnBuilders = columns.stream()
                .map(OpenSearchColumnHandle::type)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        List<String> requiredFields = columns.stream()
                .map(OpenSearchColumnHandle::name)
                .filter(name -> !isBuiltinColumn(name))
                .collect(toList());

        QueryBuilder query = buildSearchQuery(
                table.constraint().transformKeys(OpenSearchColumnHandle.class::cast),
                table.query(),
                table.regexes());

        Supplier<SearchResponse> firstPageSupplier = () -> {
            Optional<String> sort = defaultSort(table);
            return client.beginSearch(
                    split.index(),
                    split.shard(),
                    query,
                    needAllFields ? Optional.empty() : Optional.of(requiredFields),
                    documentFields,
                    Optional.empty(),
                    sort,
                    table.limit());
        };
        long start = System.nanoTime();
        SearchResponse firstPage = firstPageSupplier.get();
        readTimeNanos += System.nanoTime() - start;

        this.iterator = createIterator(client, query, split, documentFields, table.limit(), firstPage);
    }

    protected Optional<String> defaultSort(OpenSearchTableHandle table)
    {
        // sorting by _doc (index order) get special treatment in OpenSearch and is more efficient
        // However, if we're using a custom OpenSearch query, use default sorting.
        // Documents will be scored and returned based on relevance
        return table.query().isPresent() ? Optional.empty() : Optional.of("_doc");
    }

    protected abstract TrackedSearchIterator createIterator(
            OpenSearchClient client,
            QueryBuilder query,
            OpenSearchSplit split,
            List<String> documentFields,
            OptionalLong limit,
            SearchResponse firstPage);

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + iterator.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        iterator.close();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        long size = 0;
        while (size < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && iterator.hasNext()) {
            SearchHit hit = iterator.next();
            Map<String, Object> document = hit.getSourceAsMap();

            for (int i = 0; i < decoders.size(); i++) {
                OpenSearchColumnHandle columnHandle = columns.get(i);
                if (columnHandle.path().size() == 1) {
                    decoders.get(i).decode(hit, () -> getField(document, columnHandle.path().getFirst()), columnBuilders[i]);
                    continue;
                }
                Map<String, Object> resolvedField = resolveField(document, columnHandle);
                decoders.get(i)
                        .decode(
                                hit,
                                () -> resolvedField == null ? null : getField(resolvedField, columnHandle.path().getLast()),
                                columnBuilders[i]);
            }

            if (hit.getSourceRef() != null) {
                totalBytes += hit.getSourceRef().length();
            }

            size = Arrays.stream(columnBuilders)
                    .mapToLong(BlockBuilder::getSizeInBytes)
                    .sum();
        }

        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }

        return SourcePage.create(new Page(blocks));
    }

    private static Map<String, Object> resolveField(Map<String, Object> document, OpenSearchColumnHandle columnHandle)
    {
        if (document == null) {
            return null;
        }
        Map<String, Object> value = (Map<String, Object>) getField(document, columnHandle.path().getFirst());
        if (value != null) {
            for (int i = 1; i < columnHandle.path().size() - 1; i++) {
                value = (Map<String, Object>) getField(value, columnHandle.path().get(i));
                if (value == null) {
                    break;
                }
            }
        }
        return value;
    }

    public static Object getField(Map<String, Object> document, String field)
    {
        Object value = document.get(field);
        if (value == null) {
            Map<String, Object> result = new HashMap<>();
            String prefix = field + ".";
            for (Entry<String, Object> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(prefix)) {
                    result.put(key.substring(prefix.length()), entry.getValue());
                }
            }

            if (!result.isEmpty()) {
                return result;
            }
        }

        return value;
    }

    private Map<String, Type> flattenFields(List<OpenSearchColumnHandle> columns)
    {
        Map<String, Type> result = new HashMap<>();

        for (OpenSearchColumnHandle column : columns) {
            flattenFields(result, column.name(), column.type());
        }

        return result;
    }

    private void flattenFields(Map<String, Type> result, String fieldName, Type type)
    {
        if (type instanceof RowType rowType) {
            for (RowType.Field field : rowType.getFields()) {
                flattenFields(result, appendPath(fieldName, field.getName().get()), field.getType());
            }
        }
        else {
            result.put(fieldName, type);
        }
    }

    private List<Decoder> createDecoders(List<OpenSearchColumnHandle> columns)
    {
        return columns.stream()
                .map(OpenSearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }
}
