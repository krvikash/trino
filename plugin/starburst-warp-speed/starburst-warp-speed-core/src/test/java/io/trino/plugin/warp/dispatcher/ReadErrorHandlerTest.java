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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.VaradaErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.VaradaColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.RowGroupDataFilter;
import io.trino.plugin.warp.dispatcher.warmup.demoter.TupleFilter;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.ConnectorSync;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.util.NodeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumns;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReadErrorHandlerTest
{
    private RowGroupDataService rowGroupDataService;

    private RowGroupDataDao rowGroupDataDao;

    @BeforeEach
    public void beforeEach()
    {
        rowGroupDataDao = mock(RowGroupDataDao.class);
        rowGroupDataService = new RowGroupDataService(rowGroupDataDao,
                new StubsStorageEngine(),
                new GlobalConfig(),
                TestingTxService.createMetricsManager(),
                NodeUtils.mockNodeManager(),
                mock(ConnectorSync.class));
    }

    @Test
    public void testHandle_UnrecoverableError()
    {
        TrinoException trinoException = new TrinoException(VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR, "error message");
        WarmUpElement failedElement = mock(WarmUpElement.class);
        RowGroupKey rowGroupKey = mock(RowGroupKey.class);
        List<ColumnHandle> warmedColumnHandleList = mockColumns(List.of(Pair.of("c1", VarcharType.VARCHAR)));
        RowGroupData failedRowGroupData = WarmupTestDataUtil.generateRowGroupData(rowGroupKey, warmedColumnHandleList);
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        NativeQueryCollectData nativeQueryCollectData = mockNativeCollectData(failedElement);
        ImmutableList<NativeQueryCollectData> failedElementCollectData = ImmutableList.of(nativeQueryCollectData);
        QueryContext queryContext = mockQueryContext(failedElementCollectData, Collections.emptyList());

        ArgumentCaptor<List<TupleFilter>> argument = ArgumentCaptor.forClass(List.class);

        ReadErrorHandler errorHandler = new ReadErrorHandler(warmupDemoterService, rowGroupDataService, mock(PrintMetricsTimerTask.class));
        errorHandler.handle(trinoException, failedRowGroupData, queryContext);
        RowGroupDataFilter expectingResult = new RowGroupDataFilter(rowGroupKey, Set.of(failedElement));
        verify(warmupDemoterService, times(1)).tryDemoteStart(argument.capture());
        assertThat(argument.getValue()).contains(expectingResult);
    }

    @Test
    public void testHandle_ReadOutOfBoundsError()
    {
        TrinoException trinoException = new TrinoException(VaradaErrorCode.VARADA_NATIVE_READ_OUT_OF_BOUNDS, "error message");
        WarmUpElement failedElement = createWarmupElement(new RegularColumn("c1"));
        RowGroupKey rowGroupKey = mock(RowGroupKey.class);
        List<ColumnHandle> warmedColumnHandleList = mockColumns(List.of(Pair.of("c1", IntegerType.INTEGER)));
        RowGroupData failedRowGroupData = WarmupTestDataUtil.generateRowGroupData(rowGroupKey, warmedColumnHandleList);
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        NativeQueryCollectData nativeQueryCollectData = mockNativeCollectData(failedElement);
        ImmutableList<NativeQueryCollectData> failedElementCollectData = ImmutableList.of(nativeQueryCollectData);
        QueryContext queryContext = mockQueryContext(failedElementCollectData, Collections.emptyList());
        ReadErrorHandler errorHandler = new ReadErrorHandler(warmupDemoterService, rowGroupDataService, mock(PrintMetricsTimerTask.class));
        errorHandler.handle(trinoException, failedRowGroupData, queryContext);
        verify(warmupDemoterService, never()).tryDemoteStart(anyList());
        ArgumentCaptor<RowGroupData> argument = ArgumentCaptor.forClass(RowGroupData.class);
        verify(rowGroupDataDao, times(1)).save(argument.capture());
        Set<WarmUpElementState> actualFailure = argument.getValue().getWarmUpElements().stream().map(WarmUpElement::getState).collect(Collectors.toSet());
        assertThat(actualFailure).isEqualTo(Set.of(WarmUpElementState.FAILED_PERMANENTLY));
    }

    private QueryContext mockQueryContext(ImmutableList<NativeQueryCollectData> failedElementCollectData,
            List<QueryMatchData> matchColumns)
    {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getNativeQueryCollectDataList()).thenReturn(failedElementCollectData);
        when(queryContext.getMatchLeavesDFS()).thenReturn(matchColumns);
        return queryContext;
    }

    private NativeQueryCollectData mockNativeCollectData(WarmUpElement failedElement)
    {
        NativeQueryCollectData nativeQueryCollectData = mock(NativeQueryCollectData.class);
        when(nativeQueryCollectData.getWarmUpElementOptional()).thenReturn(Optional.of(failedElement));
        return nativeQueryCollectData;
    }

    private WarmUpElement createWarmupElement(VaradaColumn columnName)
    {
        return WarmUpElement.builder()
                .varadaColumn(columnName)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .warmupElementStats(new WarmupElementStats(1, -100, 100))
                .state(WarmUpElementState.VALID)
                .build();
    }
}
