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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.MetricsConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorPageSource;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.juffer.StorageEngineTxService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.MetricsRegistry;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.QueryArgs;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.GpuPageSourceSupport;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.torin.ConnectorGpuMemoryContext;
import io.trino.spi.torin.ConnectorGpuPageSource;
import io.trino.spi.torin.EmptyGpuPageSource;
import io.trino.spi.torin.IoExecutor;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingMetadata;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.generateRowGroupData;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockDispatcherTableHandle;
import static io.trino.plugin.warp.util.NodeUtils.mockNodeManager;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatcherPageSourceProviderTest
{
    private GlobalConfig globalConfig;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private MetricsManager metricsManager;
    private StorageEngineConstants storageEngineConstants;
    private WorkerWarmingService workerWarmingService;
    private ConnectorSession connectorSession;
    private DispatcherTableHandle dispatcherTableHandle;
    private List<ColumnHandle> columnHandles;
    private DispatcherSplit dispatcherSplit;
    private RowGroupDataService rowGroupDataService;
    private RowGroupKey rowGroupKey;

    private DispatcherPageSourceProvider dispatcherPageSourceProvider;
    private ConnectorPageSourceProvider proxiedPageSourceProvider;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private QueryClassifier queryClassifier;
    private Cache<DispatcherPageSourceProvider.ScanFilteringKey, FilteringStats> filteringStatsCache;
    private final SchemaTableName schemaTableName = new SchemaTableName("s", "t");

    DynamicFilter dynamicFilter;

    @BeforeEach
    public void before()
    {
        connectorTransactionHandle = mock(ConnectorTransactionHandle.class);

        globalConfig = new GlobalConfig();

        MetricsConfig metricsConfig = new MetricsConfig();
        metricsConfig.setEnabled(false);

        metricsManager = new MetricsManager(new MetricsRegistry(new CatalogNameProvider("catalog-name"), metricsConfig));
        workerWarmingService = mock(WorkerWarmingService.class);
        StorageEngine storageEngine = new StubsStorageEngine();
        storageEngineConstants = spy(new StubsStorageEngineConstants());
        connectorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new WarpSessionProperties(new SharedConfig(), new GlobalConfig()).getSessionProperties())
                .build();
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(dispatcherProxiedConnectorTransformer.isValidForAcceleration(any())).thenReturn(true);

        columnHandles = mockColumns(
                dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("C1", VarcharType.VARCHAR)));
        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = WarmupTestDataUtil.mockConnectorSplit();
        dispatcherSplit = dispatcherSplitRowGroupKeyPair.getLeft();
        rowGroupKey = dispatcherSplitRowGroupKeyPair.getRight();
        rowGroupDataService = spy(new RowGroupDataService(
                mock(RowGroupDataDao.class),
                storageEngine,
                globalConfig,
                metricsManager,
                mockNodeManager(),
                new CatalogNameProvider("catalog-name")));
        rowGroupKey = rowGroupDataService.createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        proxiedPageSourceProvider = mock(ConnectorPageSourceProvider.class);

        queryClassifier = mock(QueryClassifier.class);

        filteringStatsCache = buildFilteringStatsCache();

        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName(), TupleDomain.all());

        dispatcherPageSourceProvider = createDispatcherPageSourceProvider();

        dynamicFilter = DynamicFilter.EMPTY;
    }

    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(
                ConnectorPageSourceProvider.class,
                DispatcherPageSourceProvider.class,
                Set.of(
                        // Deprecated methods
                        ConnectorPageSourceProvider.class.getMethod(
                                "createPageSource",
                                ConnectorTransactionHandle.class,
                                ConnectorSession.class,
                                ConnectorSplit.class,
                                ConnectorTableHandle.class,
                                Optional.class,
                                List.class,
                                DynamicFilter.class)));
    }

    @Test
    public void testCreateGpuPageSourceReadsFromProxiedConnector()
    {
        RecordingGpuPageSourceProvider proxiedProvider = new RecordingGpuPageSourceProvider();
        DispatcherPageSourceProvider provider = createGpuDispatcherPageSourceProvider(proxiedProvider);
        Optional<ConnectorTableCredentials> tableCredentials = Optional.of(new ConnectorTableCredentials() {});
        DynamicFilter gpuDynamicFilter = new CompletedDynamicFilter(TupleDomain.none());
        ConnectorGpuMemoryContext memoryContext = _ -> {
            throw new UnsupportedOperationException();
        };
        IoExecutor ioExecutor = new IoExecutor()
        {
            @Override
            public <T> CompletableFuture<T> submit(Callable<T> task)
            {
                throw new UnsupportedOperationException();
            }
        };

        Optional<ConnectorGpuPageSource> result = provider.createGpuPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                tableCredentials,
                columnHandles,
                gpuDynamicFilter,
                memoryContext,
                ioExecutor);

        assertThat(result).containsSame(proxiedProvider.pageSource);
        assertThat(proxiedProvider.lastTransaction).isSameAs(connectorTransactionHandle);
        assertThat(proxiedProvider.lastSession).isSameAs(connectorSession);
        assertThat(proxiedProvider.lastSplit).isSameAs(dispatcherSplit.proxyConnectorSplit());
        assertThat(proxiedProvider.lastTable).isSameAs(dispatcherTableHandle.getProxyConnectorTableHandle());
        assertThat(proxiedProvider.lastTableCredentials).isSameAs(tableCredentials);
        assertThat(proxiedProvider.lastColumns).isSameAs(columnHandles);
        assertThat(proxiedProvider.lastDynamicFilter).isSameAs(gpuDynamicFilter);
        assertThat(proxiedProvider.lastMemoryContext).isSameAs(memoryContext);
        assertThat(proxiedProvider.lastIoExecutor).isSameAs(ioExecutor);
    }

    @Test
    public void testCreateGpuPageSourcePassesThroughNonDispatcherSplitAndTable()
    {
        RecordingGpuPageSourceProvider proxiedProvider = new RecordingGpuPageSourceProvider();
        DispatcherPageSourceProvider provider = createGpuDispatcherPageSourceProvider(proxiedProvider);
        ConnectorSplit rawSplit = new ConnectorSplit() {};
        ConnectorTableHandle rawTable = new ConnectorTableHandle() {};
        Optional<ConnectorTableCredentials> tableCredentials = Optional.of(new ConnectorTableCredentials() {});
        DynamicFilter gpuDynamicFilter = new CompletedDynamicFilter(TupleDomain.none());
        ConnectorGpuMemoryContext memoryContext = _ -> {
            throw new UnsupportedOperationException();
        };
        IoExecutor ioExecutor = new IoExecutor()
        {
            @Override
            public <T> CompletableFuture<T> submit(Callable<T> task)
            {
                throw new UnsupportedOperationException();
            }
        };

        Optional<ConnectorGpuPageSource> result = provider.createGpuPageSource(
                connectorTransactionHandle,
                connectorSession,
                rawSplit,
                rawTable,
                tableCredentials,
                columnHandles,
                gpuDynamicFilter,
                memoryContext,
                ioExecutor);

        assertThat(result).containsSame(proxiedProvider.pageSource);
        assertThat(proxiedProvider.lastTransaction).isSameAs(connectorTransactionHandle);
        assertThat(proxiedProvider.lastSession).isSameAs(connectorSession);
        assertThat(proxiedProvider.lastSplit).isSameAs(rawSplit);
        assertThat(proxiedProvider.lastTable).isSameAs(rawTable);
        assertThat(proxiedProvider.lastTableCredentials).isSameAs(tableCredentials);
        assertThat(proxiedProvider.lastColumns).isSameAs(columnHandles);
        assertThat(proxiedProvider.lastDynamicFilter).isSameAs(gpuDynamicFilter);
        assertThat(proxiedProvider.lastMemoryContext).isSameAs(memoryContext);
        assertThat(proxiedProvider.lastIoExecutor).isSameAs(ioExecutor);
    }

    @Test
    public void testGpuPageSourceSupportComesFromProxiedConnector()
    {
        RecordingPageSourceProviderFactory proxiedFactory = new RecordingPageSourceProviderFactory();
        DispatcherPageSourceProviderFactory factory = new DispatcherPageSourceProviderFactory(
                proxiedFactory,
                createWarpDispatcherPageSourceFactory(),
                mock(StorageEngineTxService.class),
                metricsManager,
                globalConfig,
                new CatalogName("warp"));

        assertThat(factory.getGpuPageSourceSupport(dispatcherTableHandle, columnHandles)).isEqualTo(GpuPageSourceSupport.SUPPORTED);
        assertThat(proxiedFactory.lastTable).isSameAs(dispatcherTableHandle.getProxyConnectorTableHandle());
        assertThat(proxiedFactory.lastColumns).isSameAs(columnHandles);

        ConnectorTableHandle proxiedTableHandle = new ConnectorTableHandle() {};
        assertThat(factory.getGpuPageSourceSupport(proxiedTableHandle, columnHandles)).isEqualTo(GpuPageSourceSupport.SUPPORTED);
        assertThat(proxiedFactory.lastTable).isSameAs(proxiedTableHandle);
    }

    @SuppressWarnings("CheckReturnValue")
    @Test
    public void testReadFlow_ColumnNotWarm_ReturnProxiedPageSourceProvider()
            throws IOException
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(anyList(), eq(dispatcherTableHandle), any(DynamicFilter.class), any(ConnectorSession.class)))
                .thenReturn(queryContext);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any())).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        try (ConnectorPageSource wrapperPageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandles,
                dynamicFilter,
                MemoryContext.NO_LIMIT)) {
            assertThat(((DispatcherWrapperPageSource) wrapperPageSource).getConnectorPageSource())
                    .isInstanceOf(TestingConnectorPageSource.class);
        }
    }

    @Test
    public void testReadFlow_FileNotExist_ReturnProxiedPageSourceProvider()
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        mockQueryClassifier(true, false, false);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any())).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandles,
                dynamicFilter,
                MemoryContext.NO_LIMIT);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(TestingConnectorPageSource.class);
    }

    @Test
    public void testReadFlow_ProxiedPageSourceReportsToMemoryContext()
            throws IOException
    {
        mockQueryClassifier(true, false, false);
        proxiedPageSourceProvider = new ConnectorPageSourceProvider()
        {
            @Override
            public ConnectorPageSource createPageSource(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorSplit split,
                    ConnectorTableHandle table,
                    Optional<ConnectorTableCredentials> tableCredentials,
                    List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter,
                    MemoryContext memoryContext)
            {
                memoryContext.setBytes(1234);
                return new EmptyPageSource();
            }
        };
        dispatcherPageSourceProvider = createDispatcherPageSourceProvider();

        AtomicLong reservedBytes = new AtomicLong();
        try (ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandles,
                dynamicFilter,
                reservedBytes::set)) {
            assertThat(pageSource.getNextSourcePage()).isNull();
        }
        assertThat(reservedBytes).hasValue(1234);
    }

    @Test
    public void testReadFlow_FileNotExistWithRuleReturnSendWarmRequest()
            throws IOException
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(anyList(), eq(dispatcherTableHandle), any(DynamicFilter.class), any(ConnectorSession.class)))
                .thenReturn(queryContext);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any())).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        try (ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandles,
                dynamicFilter,
                MemoryContext.NO_LIMIT)) {
            assertThat(((DispatcherWrapperPageSource) pageSource).getConnectorPageSource())
                    .isInstanceOf(TestingConnectorPageSource.class);
            verify(workerWarmingService, times(1))
                    .warm(any(), any(), any(), any(), any(), eq(Optional.empty()), anyList(), any(), anyInt());
        }
    }

    @Test
    public void testReadFlow_NewWarmedColumnDefinitionSendWarmRequest()
            throws IOException
    {
        List<String> columnsStr = List.of("C0");
        List<Pair<String, Type>> columnsMetadata = columnsStr.stream().map(colName -> Pair.of(colName, (Type) IntegerType.INTEGER)).collect(Collectors.toList());

        List<ColumnHandle> allColumns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);

        mockQueryClassifier(false, false, false);
        RowGroupData rowGroupDataWarmedUpElements = generateRowGroupData(rowGroupKey, columnHandles);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupDataWarmedUpElements);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupDataWarmedUpElements);

        try (ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                allColumns,
                dynamicFilter,
                MemoryContext.NO_LIMIT)) {
            assertThat(((DispatcherWrapperPageSource) pageSource).getConnectorPageSource())
                    .isInstanceOf(DispatcherPageSource.class);

            verify(workerWarmingService, times(1))
                    .warm(any(), any(), any(), any(), any(), eq(Optional.empty()), anyList(), any(), anyInt());
        }
    }

    @Test
    public void testReadFlow_FileExist_EmptyColumnQuery_WarmColumnExist()
    {
        mockQueryClassifier(false, false, true);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);

        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(PrefilledPageSource.class);
    }

    @Test
    public void testReadFlow_FileExist_AllColumnsAreWarmed()
    {
        Type columnType = IntegerType.INTEGER;
        ImmutableList<Pair<String, Type>> columnsMetadata = ImmutableList.of(Pair.of("c1", columnType));
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);
        ColumnHandle columnHandle = columns.getFirst();
        TupleDomain<ColumnHandle> proxiedPredicate = TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.singleValue(columnType, 3L)));
        TupleDomain<ColumnHandle> predicate = proxiedPredicate.transformKeys(ColumnHandle.class::cast);

        dispatcherTableHandle = mockDispatcherTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                predicate);
        dispatcherPageSourceProvider = createDispatcherPageSourceProvider();

        mockQueryClassifier(false, true, false);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);

        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columns,
                new CompletedDynamicFilter(predicate),
                MemoryContext.NO_LIMIT);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.WARP);
    }

    @Test
    public void testScansOfTheSameShapeShareAFilteringGate()
    {
        Type columnType = IntegerType.INTEGER;
        ImmutableList<Pair<String, Type>> columnsMetadata = ImmutableList.of(Pair.of("c1", columnType));
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);
        ColumnHandle columnHandle = columns.getFirst();
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.singleValue(columnType, 3L)));
        TupleDomain<ColumnHandle> otherPredicate = TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.singleValue(columnType, 4L)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        createWarpPageSource(predicate, columns);
        assertThat(filteringStatsCache.size()).isEqualTo(1);

        createWarpPageSource(predicate, columns);
        assertThat(filteringStatsCache.size()).isEqualTo(1);

        createWarpPageSource(otherPredicate, columns);
        assertThat(filteringStatsCache.size()).isEqualTo(2);
    }

    @Test
    public void testScansWithDifferentDynamicFilterColumnsDoNotShareAFilteringGate()
    {
        Type columnType = IntegerType.INTEGER;
        ImmutableList<Pair<String, Type>> columnsMetadata = ImmutableList.of(Pair.of("c1", columnType), Pair.of("c2", columnType));
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);
        List<ColumnHandle> collectColumns = ImmutableList.of(columns.getFirst());
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(columns.getFirst(), Domain.singleValue(columnType, 3L)));
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(columns.get(1), Domain.singleValue(columnType, 5L)));
        TupleDomain<ColumnHandle> dynamicFilterOnTheSameColumn = TupleDomain.withColumnDomains(Map.of(columns.get(1), Domain.singleValue(columnType, 6L)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        createWarpPageSource(predicate, dynamicFilter, collectColumns);
        assertThat(filteringStatsCache.size()).isEqualTo(1);

        createWarpPageSource(predicate, dynamicFilterOnTheSameColumn, collectColumns);
        assertThat(filteringStatsCache.size()).isEqualTo(1);

        createWarpPageSource(predicate, predicate, collectColumns);
        assertThat(filteringStatsCache.size()).isEqualTo(2);
    }

    private void createWarpPageSource(TupleDomain<ColumnHandle> predicate, List<ColumnHandle> columns)
    {
        createWarpPageSource(predicate, predicate, columns);
    }

    private void createWarpPageSource(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> dynamicFilterPredicate, List<ColumnHandle> columns)
    {
        dispatcherTableHandle = mockDispatcherTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                predicate);
        mockQueryClassifier(false, true, false);
        dispatcherPageSourceProvider = createDispatcherPageSourceProvider();

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columns,
                new CompletedDynamicFilter(dynamicFilterPredicate),
                MemoryContext.NO_LIMIT);
        assertThat(((DispatcherWrapperPageSource) pageSource).getConnectorPageSource()).isInstanceOf(DispatcherPageSource.class);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithoutPredicate()
    {
        List<Pair<String, Type>> columnsMetadata = List.of(
                Pair.of("c1", VarcharType.VARCHAR),
                Pair.of("c2", IntegerType.INTEGER));
        Type columnType = columnsMetadata.getFirst().getRight();
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);

        ColumnHandle connectorColumnHandle = columns.getFirst();
        TupleDomain<ColumnHandle> proxiedPredicate = TupleDomain.withColumnDomains(
                Map.of(
                        connectorColumnHandle, Domain.singleValue(columnType, Slices.wrappedBuffer("value".getBytes(Charset.defaultCharset())))));
        TupleDomain<ColumnHandle> predicate = proxiedPredicate.transformKeys(ColumnHandle.class::cast);

        dispatcherTableHandle = mockDispatcherTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                predicate);
        dispatcherPageSourceProvider = createDispatcherPageSourceProvider();

        mockQueryClassifier(false, false, false);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                any(DispatcherSplit.class),
                isA(TestingMetadata.TestingTableHandle.class),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any()))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columns,
                new CompletedDynamicFilter(predicate),
                MemoryContext.NO_LIMIT);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithOnlyPredicateOnProxied()
    {
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("C1", VarcharType.VARCHAR),
                        Pair.of("C2", IntegerType.INTEGER)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        mockQueryClassifier(false, false, false);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                eq(dispatcherSplit.proxyConnectorSplit()),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any()))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columns,
                dynamicFilter,
                MemoryContext.NO_LIMIT);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithPredicateOnWarp()
    {
        Type columnType = IntegerType.INTEGER;
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("c1", columnType),
                        Pair.of("c2", VarcharType.VARCHAR)));

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                Map.of(columns.getFirst(), Domain.singleValue(columnType, 3L)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        mockQueryClassifier(false, false, false);

        when(proxiedPageSourceProvider.createPageSource(
                eq(connectorTransactionHandle),
                eq(connectorSession),
                eq(dispatcherSplit.proxyConnectorSplit()),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class),
                any()))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherPageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columns,
                new CompletedDynamicFilter(predicate),
                MemoryContext.NO_LIMIT);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    private DispatcherPageSourceProvider createDispatcherPageSourceProvider()
    {
        return new DispatcherPageSourceProvider(
                proxiedPageSourceProvider,
                createWarpDispatcherPageSourceFactory(),
                mock(StorageEngineTxService.class),
                metricsManager,
                globalConfig,
                "warp",
                filteringStatsCache);
    }

    private WarpDispatcherPageSourceFactory createWarpDispatcherPageSourceFactory()
    {
        NativeStorageStateHandler nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);

        QueryArgs queryArgs = mock(QueryArgs.class);
        StorageCollectorService storageCollectorService = mock(StorageCollectorService.class);
        when(storageCollectorService.getQueryArgs(any(), any())).thenReturn(queryArgs);
        return new WarpDispatcherPageSourceFactory(
                storageEngineConstants,
                rowGroupDataService,
                metricsManager,
                dispatcherProxiedConnectorTransformer,
                mock(PredicatesCacheService.class),
                queryClassifier,
                globalConfig,
                new ReadErrorHandler(rowGroupDataService, mock(PrintMetricsTimerTask.class)),
                mock(CollectTxService.class),
                storageCollectorService,
                mock(MatchService.class),
                workerWarmingService,
                new WorkerMemoryManager(
                        new CatalogName("f"),
                        new ShapingLoggerFactory(new CatalogName("f"), new SharedConfig())),
                nativeStorageStateHandler,
                new ShapingLoggerFactory(new CatalogName("catalog-name"), new SharedConfig()));
    }

    @SuppressModernizer // entries are never invalidated, so the eviction races that EvictableCacheBuilder guards against cannot happen
    private static Cache<DispatcherPageSourceProvider.ScanFilteringKey, FilteringStats> buildFilteringStatsCache()
    {
        return CacheBuilder.newBuilder().build();
    }

    protected void mockQueryClassifier(boolean isProxyOnly, boolean isWarpOnly, boolean isPrefilledOnly)
    {
        QueryContext basicQueryContext = mock(QueryContext.class);
        when(basicQueryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(
                anyList(),
                eq(dispatcherTableHandle),
                any(DynamicFilter.class),
                any(ConnectorSession.class)))
                .thenReturn(basicQueryContext);
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getNativeQueryCollectDataList()).thenReturn(ImmutableList.of());
        when(queryContext.getPrefilledQueryCollectDataByBlockIndex()).thenReturn(ImmutableMap.of());
        when(queryContext.getMatchData()).thenReturn(Optional.empty());
        when(queryContext.getRemainingCollectColumns()).thenReturn(ImmutableList.of());
        when(queryContext.getRemainingCollectColumnByBlockIndex()).thenReturn(ImmutableMap.of());
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE));
        when(queryContext.isProxyOnly()).thenReturn(isProxyOnly);
        when(queryContext.isWarpOnly()).thenReturn(isWarpOnly);
        when(queryContext.isPrefilledOnly()).thenReturn(isPrefilledOnly);
        when(queryClassifier.classify(
                eq(basicQueryContext),
                any(),
                eq(dispatcherTableHandle),
                eq(connectorSession)))
                .thenReturn(queryContext);
    }

    private DispatcherPageSourceProvider createGpuDispatcherPageSourceProvider(ConnectorPageSourceProvider proxiedProvider)
    {
        return new DispatcherPageSourceProvider(
                proxiedProvider,
                createWarpDispatcherPageSourceFactory(),
                mock(StorageEngineTxService.class),
                metricsManager,
                globalConfig,
                "warp",
                filteringStatsCache);
    }

    private static final class RecordingGpuPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        private final ConnectorGpuPageSource pageSource = new EmptyGpuPageSource();

        private ConnectorTransactionHandle lastTransaction;
        private ConnectorSession lastSession;
        private ConnectorSplit lastSplit;
        private ConnectorTableHandle lastTable;
        private Optional<ConnectorTableCredentials> lastTableCredentials;
        private List<ColumnHandle> lastColumns;
        private DynamicFilter lastDynamicFilter;
        private ConnectorGpuMemoryContext lastMemoryContext;
        private IoExecutor lastIoExecutor;

        @Override
        public Optional<ConnectorGpuPageSource> createGpuPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                ConnectorGpuMemoryContext memoryContext,
                IoExecutor ioExecutor)
        {
            lastTransaction = transaction;
            lastSession = session;
            lastSplit = split;
            lastTable = table;
            lastTableCredentials = tableCredentials;
            lastColumns = columns;
            lastDynamicFilter = dynamicFilter;
            lastMemoryContext = memoryContext;
            lastIoExecutor = ioExecutor;
            return Optional.of(pageSource);
        }
    }

    private static final class RecordingPageSourceProviderFactory
            implements ConnectorPageSourceProviderFactory
    {
        private ConnectorTableHandle lastTable;
        private List<ColumnHandle> lastColumns;

        @Override
        public GpuPageSourceSupport getGpuPageSourceSupport(ConnectorTableHandle connectorTableHandle, List<ColumnHandle> columns)
        {
            lastTable = connectorTableHandle;
            lastColumns = columns;
            return GpuPageSourceSupport.SUPPORTED;
        }

        @Override
        public ConnectorPageSourceProvider createPageSourceProvider(MemoryContext memoryContext)
        {
            throw new UnsupportedOperationException();
        }
    }
}
