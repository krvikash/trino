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

import io.airlift.units.DataSize;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorPageSource;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.connector.TestingConnectorTableHandle;
import io.trino.plugin.warp.connector.TestingConnectorTransactionHandle;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.classifier.ClassifierFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextFactory;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicateBufferInfo;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.CustomStatsContext;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.read.CollectTxService;
import io.trino.plugin.warp.storage.read.MatchService;
import io.trino.plugin.warp.storage.read.PrefilledPageSource;
import io.trino.plugin.warp.storage.read.QueryArgs;
import io.trino.plugin.warp.storage.read.StorageCollectorService;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.tools.util.WarpReadWriteLock;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.warp.WarpSessionProperties.ENABLE_MATCH_COLLECT;
import static io.trino.plugin.warp.WarpSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.generateRowGroupData;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.warp.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatcherPageSourceFactoryTest
{
    private WorkerWarmingService workerWarmingService;
    private RowGroupDataDao rowGroupDataDao;
    private DispatcherSplit dispatcherSplit;
    private ConnectorSession connectorSession;
    private DynamicFilter dynamicFilter;
    private ConnectorPageSourceProvider connectorPageSourceProvider;
    private TestingConnectorTransactionHandle proxiedTransactionHandle;
    private DispatcherTableHandle dispatcherTableHandle;
    private NativeStorageStateHandler nativeStorageStateHandler;
    private WarpDispatcherPageSourceFactory pageSourceFactory;
    private CustomStatsContext customStatsContext;

    @BeforeEach
    public void before()
    {
        dispatcherSplit = new DispatcherSplit(
                "database",
                "table",
                "path",
                0L,
                1L,
                2L,
                List.of(),
                "",
                mock(ConnectorSplit.class));
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), any())).thenReturn(1_000_000);
        when(connectorSession.getProperty(eq(ENABLE_MATCH_COLLECT), any())).thenReturn(true);
        connectorPageSourceProvider = mock(ConnectorPageSourceProvider.class);
        proxiedTransactionHandle = mock(TestingConnectorTransactionHandle.class);

        TestingConnectorTableHandle proxyTableHandle = new TestingConnectorTableHandle(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                List.of(),
                List.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
        dispatcherTableHandle = new DispatcherTableHandle(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                OptionalLong.of(0),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of()),
                proxyTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false,
                Set.of());

        this.customStatsContext = new CustomStatsContext(mock(MetricsManager.class), dispatcherTableHandle.getCustomStats());

        dynamicFilter = mock(DynamicFilter.class);
        when(dynamicFilter.getCurrentPredicate()).thenReturn(TupleDomain.all());

        StubsStorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        StubsStorageEngine storageEngine = new StubsStorageEngine();
        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.getQueryNullBufferSize(any())).thenReturn(64 * 1024);
        MatchCollectIdService matchCollectIdService = mock(MatchCollectIdService.class);
        rowGroupDataDao = mock(RowGroupDataDao.class);
        GlobalConfig globalConfig = new GlobalConfig();
        DictionaryConfig dictionaryConfig = new DictionaryConfig();
        dictionaryConfig.setMaxDictionaryTotalCacheWeight(DataSize.of(16L, DataSize.Unit.MEGABYTE));
        dictionaryConfig.setDictionaryCacheConcurrencyLevel(1);
        NativeConfig nativeConfig = mock(NativeConfig.class);
        when(nativeConfig.getCollectTxSize()).thenReturn(8 * 1024 * 1024);
        when(nativeConfig.getTaskMaxWorkerThreads()).thenReturn(1);
        workerWarmingService = mock(WorkerWarmingService.class);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        NodeManager nodeManager = mockNodeManager();
        RowGroupDataService rowGroupDataService = new RowGroupDataService(
                rowGroupDataDao,
                storageEngine,
                globalConfig,
                metricsManager,
                nodeManager,
                new CatalogNameProvider("catalog-name"));
        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        PredicateContextFactory predicateContextFactory = new PredicateContextFactory(globalConfig, new TestingConnectorProxiedConnectorTransformer());
        when(predicatesCacheService.predicateDataToBuffer(isA(PredicateData.class), any())).thenReturn(Optional.of(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.INVALID), Optional.empty())));
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        ClassifierFactory classifierFactory = new ClassifierFactory(
                storageEngineConstants,
                predicatesCacheService,
                bufferAllocator,
                nativeConfig,
                dispatcherProxiedConnectorTransformer,
                matchCollectIdService,
                globalConfig,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
        QueryClassifier queryClassifier = new QueryClassifier(
                classifierFactory,
                matchCollectIdService,
                predicateContextFactory,
                dispatcherProxiedConnectorTransformer,
                globalConfig);

        nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);

        QueryArgs queryArgs = mock(QueryArgs.class);
        StorageCollectorService storageCollectorService = mock(StorageCollectorService.class);
        when(storageCollectorService.getQueryArgs(any(), any())).thenReturn(queryArgs);
        GlobalConfig globalConfig2 = new GlobalConfig();
        pageSourceFactory = new WarpDispatcherPageSourceFactory(
                storageEngineConstants,
                rowGroupDataService,
                metricsManager,
                dispatcherProxiedConnectorTransformer,
                predicatesCacheService,
                queryClassifier,
                globalConfig2,
                new ReadErrorHandler(rowGroupDataService, mock(PrintMetricsTimerTask.class)),
                mock(CollectTxService.class),
                storageCollectorService,
                mock(MatchService.class),
                workerWarmingService,
                new WorkerMemoryManager(
                        new CatalogName("f"),
                        new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())),
                nativeStorageStateHandler,
                new ShapingLoggerFactory(new CatalogName("catalog-name"), new SharedConfig()));
    }

    @Test
    public void testCreateWarpConnectorPageSourceRowGroupDataIsNotWarm()
    {
        List<ColumnHandle> columnHandleList = WarmupTestDataUtil.mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));

        when(rowGroupDataDao.get(any(RowGroupKey.class))).thenReturn(null);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(null);

        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandleList,
                DynamicFilter.EMPTY,
                customStatsContext);
        verifyWarmCalled(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle);
        verify(connectorPageSourceProvider, times(1))
                .createPageSource(eq(proxiedTransactionHandle),
                        eq(connectorSession),
                        eq(dispatcherSplit.proxyConnectorSplit()),
                        eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
        assertThat(pageSource).isNull(); // not mocked but not DispatcherPageSource
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());

        assertThat(stats.getcached_proxied_files()).isEqualTo(1);
        assertThat(stats.getcached_files()).isZero();
    }

    @Test
    public void testCreateWarpConnectorPageSourceRowGroupDataIsEmpty()
    {
        List<ColumnHandle> columnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));
        long now = Instant.now().toEpochMilli();
        long creationTime = now - 1000;
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .colName("c1")
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_CHAR)
                .recTypeLength(8192)
                .lastUsedTimestamp(creationTime)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .totalRecords(0)
                .build();
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "filePath", 10, 10, Instant.now().getEpochSecond(), "", "");
        RowGroupData rowGroupData = RowGroupData.builder()
                .warmUpElements(Collections.singleton(warmUpElement))
                .rowGroupKey(rowGroupKey)
                .isEmpty(true)
                .build();

        when(rowGroupDataDao.get(any(RowGroupKey.class))).thenReturn(rowGroupData);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(rowGroupData);
        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandleList,
                DynamicFilter.EMPTY,
                customStatsContext);
        assertThat(rowGroupData.getWarmUpElements().stream().findAny().orElseThrow().getLastUsedTimestamp()).isGreaterThanOrEqualTo(now);

        verifyWarmCalled(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle);

        verify(connectorPageSourceProvider, never())
                .createPageSource(eq(proxiedTransactionHandle),
                        eq(connectorSession),
                        eq(dispatcherSplit),
                        eq(dispatcherTableHandle),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
        assertThat(pageSource).isInstanceOf(EmptyPageSource.class);
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        assertThat(stats.getcached_proxied_files()).isZero();
        assertThat(stats.getcached_files()).isZero();
    }

    @Test
    public void testCreateWarpConnectorPageSourceRowGroupDataIsLocked()
    {
        List<ColumnHandle> columnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));
        WarmUpElement validWarmUpElement = WarmUpElement.builder()
                .colName("c1")
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_CHAR)
                .recTypeLength(8192)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .totalRecords(1)
                .build();
        WarpReadWriteLock warpReadWriteLock = mock(WarpReadWriteLock.class);
        when(warpReadWriteLock.readLock()).thenReturn(false);
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "filePath", 10, 10, Instant.now().getEpochSecond(), "", "");
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .lock(warpReadWriteLock)
                .warmUpElements(Collections.singletonList(validWarmUpElement))
                .build();

        when(rowGroupDataDao.get(any(RowGroupKey.class))).thenReturn(rowGroupData);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(rowGroupData);

        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandleList,
                dynamicFilter,
                customStatsContext);
        assertThat(pageSource).isNull();
        verify(connectorPageSourceProvider, times(1))
                .createPageSource(any(ConnectorTransactionHandle.class),
                        any(ConnectorSession.class),
                        eq(dispatcherSplit.proxyConnectorSplit()),
                        eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
        verifyWarmCalled(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle);

        verify(connectorPageSourceProvider, times(1))
                .createPageSource(eq(proxiedTransactionHandle),
                        eq(connectorSession),
                        eq(dispatcherSplit.proxyConnectorSplit()),
                        eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
    }

    @Test
    public void testCreateWarpConnectorPageSourceOnlyProxy()
    {
        List<ColumnHandle> columnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));

        pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandleList,
                dynamicFilter,
                customStatsContext);
        verify(connectorPageSourceProvider, times(1))
                .createPageSource(any(ConnectorTransactionHandle.class),
                        any(ConnectorSession.class),
                        eq(dispatcherSplit.proxyConnectorSplit()),
                        eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
        verifyWarmCalled(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle);

        verify(connectorPageSourceProvider, times(1))
                .createPageSource(eq(proxiedTransactionHandle),
                        eq(connectorSession),
                        eq(dispatcherSplit.proxyConnectorSplit()),
                        eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                        eq(Optional.empty()),
                        anyList(),
                        any(DynamicFilter.class));
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        assertThat(stats.getexternal_collect_columns()).isEqualTo(columnHandleList.size());
        assertThat(stats.getexternal_match_columns()).isZero();
    }

    @Test
    public void testFailedRowGroupShouldUseProxied()
    {
        List<ColumnHandle> columnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));

        RowGroupKey rowGroupKey = createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        List<ColumnHandle> warmedColumnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));
        RowGroupData rowGroupDataToWarm = generateRowGroupData(rowGroupKey, warmedColumnHandleList);
        rowGroupDataToWarm = RowGroupData.builder(rowGroupDataToWarm)
                .warmUpElements(rowGroupDataToWarm.getWarmUpElements().stream()
                        .map(we -> WarmUpElement.builder(we)
                                .state(WarmUpElementState.FAILED_PERMANENTLY)
                                .build())
                        .collect(Collectors.toList()))
                .build();
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupDataToWarm);
        TestingConnectorPageSource proxiedPageSource = mock(TestingConnectorPageSource.class);
        when(connectorPageSourceProvider.createPageSource(
                eq(proxiedTransactionHandle),
                eq(connectorSession),
                any(DispatcherSplit.class),
                isA(ConnectorTableHandle.class),
                eq(Optional.empty()),
                anyList(),
                any(DynamicFilter.class)))
                .thenReturn(proxiedPageSource);
        List<ColumnHandle> columnHandles = Stream.concat(columnHandleList.stream(), warmedColumnHandleList.stream()).collect(Collectors.toList());
        pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandles,
                dynamicFilter,
                customStatsContext);
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        assertThat(stats.getexternal_collect_columns()).isEqualTo(columnHandles.size()); // should be uncomment when committing to develop

        verify(connectorPageSourceProvider, times(1))
                .createPageSource(any(),
                        any(),
                        any(),
                        any(),
                        eq(Optional.empty()),
                        any(),
                        any(DynamicFilter.class));
    }

    /**
     * select count(*) from T
     */
    @Test
    public void testRepresentativeColumn()
    {
        List<ColumnHandle> columnHandleList = List.of();
        List<ColumnHandle> existingColumnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));

        RowGroupKey rowGroupKey = createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        RowGroupData rowGroupDataToWarm = generateRowGroupData(rowGroupKey, existingColumnHandleList);
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupDataToWarm);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(rowGroupDataToWarm);
        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                columnHandleList,
                dynamicFilter,
                customStatsContext);
        assertThat(pageSource).isInstanceOf(PrefilledPageSource.class);
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        assertThat(stats.getexternal_collect_columns()).isEqualTo(0);
        assertThat(stats.getwarp_collect_columns()).isEqualTo(0);
        assertThat(stats.getempty_collect_columns()).isEqualTo(1);
    }

    /**
     * select count(*) from T WHERE C = 3
     */
    @Test
    public void testRepresentativeColumnWithPartitionPredicate()
    {
        List<ColumnHandle> partitionColumnHandleList = mockColumns(List.of(Pair.of("c1", IntegerType.INTEGER)));

        RowGroupKey rowGroupKey = createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        RowGroupData rowGroupDataToWarm = generateRowGroupData(rowGroupKey, partitionColumnHandleList);
        rowGroupDataToWarm = RowGroupData.builder(rowGroupDataToWarm).partitionKeys(Map.of(new RegularColumn("c1"), "3")).build();
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupDataToWarm);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(rowGroupDataToWarm);

        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                partitionColumnHandleList,
                dynamicFilter,
                customStatsContext);
        assertThat(pageSource).isInstanceOf(PrefilledPageSource.class);
        DispatcherPageSourceStats stats = (DispatcherPageSourceStats) customStatsContext.getStat(DispatcherPageSourceStats.createKey());
        assertThat(stats.getwarp_prefilled_collect_columns()).isEqualTo(1);
        assertThat(stats.getexternal_collect_columns()).isEqualTo(0);
        assertThat(stats.getwarp_collect_columns()).isEqualTo(0);
        assertThat(stats.getempty_collect_columns()).isEqualTo(0);
        assertThat(pageSource.getNextSourcePage()).isNotNull();
    }

    /**
     * \\N as partition value represents null
     * <p>
     * see io.trino.plugin.hive.HivePartitionKey
     */
    @Test
    public void testPartitionNullValue()
    {
        List<ColumnHandle> partitionColumnHandleList = mockColumns(List.of(Pair.of("c1", IntegerType.INTEGER)));
        RowGroupKey rowGroupKey = createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        RowGroupData rowGroupDataToWarm = generateRowGroupData(rowGroupKey, partitionColumnHandleList);
        rowGroupDataToWarm = RowGroupData.builder(rowGroupDataToWarm).partitionKeys(Map.of(new RegularColumn("c1"), "\\N")).build();
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupDataToWarm);
        when(rowGroupDataDao.getIfPresent(any(RowGroupKey.class))).thenReturn(rowGroupDataToWarm);

        ConnectorPageSource pageSource = pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                partitionColumnHandleList,
                dynamicFilter,
                customStatsContext);
        assertThat(pageSource).isInstanceOf(PrefilledPageSource.class);
        assertThat(pageSource.getNextSourcePage()).isNotNull();
    }

    @Test
    public void testProxyWhenNativeUnavailable()
    {
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(false);

        List<ColumnHandle> partitionColumnHandleList = mockColumns(List.of(Pair.of("c1", IntegerType.INTEGER)));
        List<ColumnHandle> regularColumnList = mockColumns(List.of(Pair.of("c2", IntegerType.INTEGER)));

        RowGroupKey rowGroupKey = createRowGroupKey(
                dispatcherSplit.schemaName(),
                dispatcherSplit.tableName(),
                dispatcherSplit.path(),
                dispatcherSplit.start(),
                dispatcherSplit.length(),
                dispatcherSplit.fileModifiedTime(),
                dispatcherSplit.deletedFilesHash());

        RowGroupData rowGroupDataToWarm = generateRowGroupData(rowGroupKey, regularColumnList);
        rowGroupDataToWarm = RowGroupData.builder(rowGroupDataToWarm).partitionKeys(Map.of(new RegularColumn("c1"), "\\N")).build();
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupDataToWarm);

        pageSourceFactory.createConnectorPageSource(
                connectorPageSourceProvider,
                proxiedTransactionHandle,
                connectorSession,
                dispatcherSplit,
                dispatcherTableHandle,
                Optional.empty(),
                partitionColumnHandleList,
                dynamicFilter,
                customStatsContext);

        verify(connectorPageSourceProvider, times(1)).createPageSource(any(), any(), any(), any(), eq(Optional.empty()), anyList(), any());
        verify(workerWarmingService, never()).warm(any(), any(), any(), any(), any(), eq(Optional.empty()), anyList(), any(), anyInt());
    }

    private void verifyWarmCalled(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            TestingConnectorTransactionHandle proxiedTransactionHandle,
            ConnectorSession connectorSession,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle)
    {
        ArgumentCaptor<ConnectorPageSourceProvider> connectorPageSourceProviderArgumentCaptor = ArgumentCaptor.forClass(ConnectorPageSourceProvider.class);
        ArgumentCaptor<TestingConnectorTransactionHandle> transactionHandleArgumentCaptor = ArgumentCaptor.forClass(TestingConnectorTransactionHandle.class);
        ArgumentCaptor<ConnectorSession> connectorSessionArgumentCaptor = ArgumentCaptor.forClass(ConnectorSession.class);
        ArgumentCaptor<DispatcherSplit> dispatcherSplitArgumentCaptor = ArgumentCaptor.forClass(DispatcherSplit.class);
        ArgumentCaptor<DispatcherTableHandle> dispatcherTableHandleArgumentCaptor = ArgumentCaptor.forClass(DispatcherTableHandle.class);

        verify(workerWarmingService, times(1))
                .warm(connectorPageSourceProviderArgumentCaptor.capture(),
                        transactionHandleArgumentCaptor.capture(),
                        connectorSessionArgumentCaptor.capture(),
                        dispatcherSplitArgumentCaptor.capture(),
                        dispatcherTableHandleArgumentCaptor.capture(),
                        eq(Optional.empty()),
                        anyList(),
                        any(),
                        anyInt());
        assertThat(connectorPageSourceProvider).isEqualTo(connectorPageSourceProviderArgumentCaptor.getValue());
        assertThat(proxiedTransactionHandle).isEqualTo(transactionHandleArgumentCaptor.getValue());
        assertThat(connectorSession).isEqualTo(connectorSessionArgumentCaptor.getValue());
        assertThat(dispatcherSplit).isEqualTo(dispatcherSplitArgumentCaptor.getValue());
        assertThat(dispatcherTableHandle).isEqualTo(dispatcherTableHandleArgumentCaptor.getValue());
    }

    private RowGroupKey createRowGroupKey(
            String schema,
            String table,
            String path,
            long start,
            long length,
            long fileModifiedTime,
            String deletedFilesHash)
    {
        return new RowGroupKey(
                schema,
                table,
                path,
                start,
                length,
                fileModifiedTime,
                deletedFilesHash,
                "");
    }
}
