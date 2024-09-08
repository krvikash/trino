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
package io.trino.plugin.warp.dispatcher;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import io.airlift.slice.Slice;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.connector.TestingConnectorTableHandle;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.PredicateInfo;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createRangeQuery;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WarmupTestDataUtil
{
    private WarmupTestDataUtil()
    {
    }

    public static List<ColumnHandle> mockColumns(
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            List<Pair<String, Type>> columnsMetadata)
    {
        List<ColumnHandle> ret = mockColumns(columnsMetadata);
        ret.forEach((ch) -> {
            TestingConnectorColumnHandle testingConnectorColumnHandle = (TestingConnectorColumnHandle) ch;
            Type columnType = testingConnectorColumnHandle.type();
            when(dispatcherProxiedConnectorTransformer.getWarpRegularColumn(eq(ch))).thenReturn(new RegularColumn(testingConnectorColumnHandle.name()));
            when(dispatcherProxiedConnectorTransformer.getColumnType(eq(ch))).thenReturn(columnType);
        });
        return ret;
    }

    public static List<ColumnHandle> mockColumns(List<Pair<String, Type>> columnsMetadata)
    {
        return columnsMetadata.stream()
                .map(pair -> new TestingConnectorColumnHandle(pair.getRight(), pair.getLeft()))
                .collect(Collectors.toList());
    }

    public static List<WarpColumn> createRegularColumns(List<ColumnHandle> columnHandles, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        return columnHandles.stream()
                .map(dispatcherProxiedConnectorTransformer::getWarpRegularColumn)
                .collect(Collectors.toList());
    }

    public static Pair<DispatcherSplit, RowGroupKey> mockConnectorSplit()
    {
        return mockConnectorSplit("database", "table");
    }

    public static Pair<DispatcherSplit, RowGroupKey> mockConnectorSplit(String schemaName, String tableName)
    {
        String filePath = "s3://a/part1-" + new Random().nextInt();
        DispatcherSplit dispatcherSplit = new DispatcherSplit(schemaName,
                tableName,
                filePath,
                0L,
                0L,
                2L,
                List.of(),
                List.of(),
                "",
                mock(ConnectorSplit.class));
        RowGroupKey rowGroupKey = new RowGroupKey(dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash(),
                "");
        return Pair.of(dispatcherSplit, rowGroupKey);
    }

    public static RowGroupData generateRowGroupData(RowGroupKey rowGroupKey,
            List<ColumnHandle> columnHandles)
    {
        List<WarmUpType> warmUpTypeData = List.of(WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        return generateRowGroupData(columnHandles,
                warmUpTypeData,
                rowGroupKey,
                false);
    }

    public static RowGroupData generateRowGroupData(List<ColumnHandle> columnHandles,
            List<WarmUpType> warmUpTypes,
            RowGroupKey rowGroupKey,
            boolean isEmpty)
    {
        return generateRowGroupData(columnHandles,
                warmUpTypes.stream().collect(Collectors.toMap(Function.identity(), c -> WarmUpElementState.VALID)),
                rowGroupKey,
                isEmpty);
    }

    public static RowGroupData generateRowGroupData(List<ColumnHandle> columnHandles,
            Map<WarmUpType, WarmUpElementState> warmUpTypeToState,
            RowGroupKey rowGroupKey,
            boolean isEmpty)
    {
        checkArgument(!warmUpTypeToState.isEmpty() && !columnHandles.isEmpty());
        List<WarmUpElement> warmUpElementsData = columnHandles.stream()
                .map(TestingConnectorColumnHandle.class::cast)
                .flatMap(c -> warmUpTypeToState.entrySet().stream().map(entry ->
                        WarmUpElement.builder()
                                .warmUpType(entry.getKey())
                                .recTypeCode(TypeUtils.convertToRecTypeCode(c.type(), TypeUtils.getTypeLength(c.type(), 8192), 8))
                                .recTypeLength(TypeUtils.getTypeLength(c.type(), 8192))
                                .colName(c.name())
                                .totalRecords(isEmpty ? 0 : 2)
                                .state(entry.getValue())
                                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                                .build()))
                .collect(Collectors.toList());
        return RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElementsData)
                .isEmpty(isEmpty)
                .build();
    }

    public static DispatcherTableHandle mockDispatcherTableHandle(String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> fullPredicate)
    {
        TestingConnectorTableHandle testingConnectorTableHandle = mock(TestingConnectorTableHandle.class);
        when(testingConnectorTableHandle.getTableParameters()).thenReturn(Optional.empty());
        when(testingConnectorTableHandle.getPartitionColumns()).thenReturn(Collections.emptyList());
        when(testingConnectorTableHandle.getPartitions()).thenReturn(Optional.empty());
        when(testingConnectorTableHandle.getAnalyzeColumnNames()).thenReturn(Optional.empty());
        when(testingConnectorTableHandle.getMaxScannedFileSize()).thenReturn(Optional.empty());

        return new DispatcherTableHandle(
                schemaName,
                tableName,
                OptionalLong.of(0),
                fullPredicate,
                new SimplifiedColumns(Set.of()),
                testingConnectorTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);
    }

    public static SetMultimap<WarpColumn, WarmupProperties> createRequiredWarmUpTypes(List<ColumnHandle> columns, List<WarmUpType> warmUpTypeList)
    {
        SetMultimap<WarpColumn, WarmupProperties> warmUpTypeMap = HashMultimap.create();
        columns.forEach(column -> warmUpTypeList.forEach(type -> warmUpTypeMap.put(new RegularColumn(((TestingConnectorColumnHandle) column).name()),
                new WarmupProperties(type, 1, 0, TransformFunction.NONE))));
        return warmUpTypeMap;
    }

    public static TestingConnectorColumnHandle mockColumnHandle(String name, Type type, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(type, name);
        when(dispatcherProxiedConnectorTransformer.getColumnType(eq(columnHandle))).thenReturn(type);
        return columnHandle;
    }

    public static List<WarmUpElement> createRegularWarmupElements(Multimap<WarpColumn, WarmUpType> columnNameToWarmUpType)
    {
        Multimap<WarpColumn, WarmUpType> columnToWarmUpType = ArrayListMultimap.create();
        for (Map.Entry<WarpColumn, WarmUpType> entry : columnNameToWarmUpType.entries()) {
            columnToWarmUpType.put(entry.getKey(), entry.getValue());
        }
        return createWarmupElements(columnToWarmUpType);
    }

    public static List<WarmUpElement> createWarmupElements(Multimap<WarpColumn, WarmUpType> columnToWarmUpType)
    {
        return columnToWarmUpType.entries().stream()
                .map(entry -> createWarmupElement(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public static WarmUpElement createWarmupElement(WarpColumn warpColumn, WarmUpType warmUpType)
    {
        return WarmUpElement.builder()
                .warmUpType(warmUpType)
                .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                .recTypeLength(4)
                .totalRecords(10)
                .warpColumn(warpColumn)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
    }

    public static List<WarmupRule> createWarmupRules(SetMultimap<WarpColumn, WarmUpType> columnNameToWarmUpType,
            SchemaTableName schemaTableName)
    {
        return columnNameToWarmUpType.entries()
                .stream()
                .map(entry -> WarmupRule.builder()
                        .schema(schemaTableName.getSchemaName())
                        .table(schemaTableName.getTableName())
                        .warpColumn(entry.getKey())
                        .warmUpType(entry.getValue())
                        .priority(2)
                        .ttl(2)
                        .predicates(Collections.emptySet())
                        .build())
                .toList();
    }

    public static LuceneQueryMatchData createLuceneQueryMatchData(WarmUpElement luceneWarmUpElement,
            boolean tightnessRequired,
            Set<Range> orderedRanges,
            Set<Slice> likeQueries,
            Domain domain,
            boolean collectNulls)
    {
        PredicateInfo predicateInfo = new PredicateInfo(PredicateType.PREDICATE_TYPE_LUCENE, FunctionType.FUNCTION_TYPE_NONE, 0, Collections.emptyList(), 0);
        PredicateData predicateData = PredicateData
                .builder()
                .predicateInfo(predicateInfo)
                .isCollectNulls(collectNulls)
                .columnType(VarcharType.VARCHAR)
                .build();
        return createLuceneQueryMatchData(luceneWarmUpElement, tightnessRequired, orderedRanges, likeQueries, domain, predicateData);
    }

    public static LuceneQueryMatchData createLuceneQueryMatchData(WarmUpElement luceneWarmUpElement,
            boolean tightnessRequired,
            Set<Range> orderedRanges,
            Set<Slice> likeQueries,
            Domain domain,
            PredicateData predicateData)
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        likeQueries.forEach(likeQuery -> queryBuilder.add(new BooleanClause(createLikeQuery(likeQuery), BooleanClause.Occur.MUST)));
        BooleanQuery.Builder innerQueryBuilder = new BooleanQuery.Builder();
        orderedRanges.forEach(range -> innerQueryBuilder.add(new BooleanClause(createRangeQuery(range), BooleanClause.Occur.SHOULD)));
        queryBuilder.add(innerQueryBuilder.build(), BooleanClause.Occur.MUST);

        return LuceneQueryMatchData.builder()
                .warmUpElement(luceneWarmUpElement)
                .type(VarcharType.createVarcharType(10))
                .domain(Optional.of(domain))
                .tightnessRequired(tightnessRequired)
                .query(queryBuilder.build())
                .collectNulls(predicateData.isCollectNulls())
                .build();
    }

    public static BufferAllocator mockBufferAllocator(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            NativeConfig nativeConfig,
            MetricsManager metricsManager)
    {
        int defaultSize = 1 << 12;
        BufferAllocator bufferAllocator = spy(new BufferAllocator(
                storageEngine,
                storageEngineConstants,
                nativeConfig,
                mock(ConnectorSync.class),
                metricsManager,
                new WarpInitializedServiceRegistry()));
        bufferAllocator.init();

        int numSegments = JbufType.JBUF_TYPE_NUM_OF.ordinal();
        MemorySegment[] segments = new MemorySegment[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = Arena.global().allocate(defaultSize);
        }

        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).ids2NullBuff(any());
        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).memorySegment2NullBuff(any());
        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).ids2RecBuff(any());
        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).memorySegment2RecBuff(any());
        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).memorySegment2CrcBuff(any());
        doReturn(allocateByteBuffer(defaultSize)).when(bufferAllocator).memorySegment2ExtRecsBuff(any());
        doReturn(allocateIntBuffer(defaultSize)).when(bufferAllocator).memorySegment2VarlenMdBuff(any());

        doReturn(segments).when(bufferAllocator).getWarmBuffers(any());
        doReturn(new long[JbufType.JBUF_TYPE_NUM_OF.ordinal()]).when(bufferAllocator).getQueryIdsArray();
        return bufferAllocator;
    }

    private static ByteBuffer allocateByteBuffer(int capacity)
    {
        return ByteBuffer.allocate(capacity);
    }

    private static IntBuffer allocateIntBuffer(int capacity)
    {
        return IntBuffer.allocate(capacity);
    }
}
