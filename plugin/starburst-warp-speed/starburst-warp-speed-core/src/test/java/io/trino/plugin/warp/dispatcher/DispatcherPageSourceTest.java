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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.query.data.QueryColumn;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.storage.read.WarpPageSource;
import io.trino.plugin.warp.storage.read.WarpStoragePageSource;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.warp.storage.read.WarpStoragePageSource.RowRanges;
import static io.trino.plugin.warp.storage.write.StorageWriterServiceTest.buildLongPage;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatcherPageSourceTest
{
    protected WarpStoragePageSource warpPageSource;
    protected ConnectorPageSource proxiedConnectorPageSource;
    protected RowGroupData rowGroupData;
    protected final DispatcherPageSourceStats stats = new DispatcherPageSourceStats("test");
    protected final Map<Integer, Type> proxiedCollectTypeByBlockIndex = Map.of(1, BIGINT);
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    private RowGroupCloseHandler closeHandler;
    private ReadErrorHandler readErrorHandler;
    private boolean assertNextPageMetrics = true;

    @BeforeEach
    public void before()
    {
        proxiedConnectorPageSource = null;
        rowGroupData = mock(RowGroupData.class);
        closeHandler = mock(RowGroupCloseHandler.class);
        readErrorHandler = mock(ReadErrorHandler.class);
        this.dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(rowGroupData.isEmpty()).thenReturn(false);
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "/", 0, 0, 0, "", "");
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
    }

    private void prepareMock(List<TestPage> testPages, List<Page> proxiedPages)
    {
        warpPageSource = new TestingWarpPageSource(testPages);
        proxiedConnectorPageSource = new FixedPageSource(proxiedPages);
    }

    @Test
    public void testRowGroupRemovalOnNativeException()
    {
        warpPageSource = mock(WarpPageSource.class);
        when(warpPageSource.getNextPage())
                .thenThrow(new TrinoException(WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR, "message"));

        DispatcherPageSource mixQueryWithPredicate = getDispatcherPageSource(2, Map.of(0, IntegerType.INTEGER));

        assertThatThrownBy(mixQueryWithPredicate::getNextPage)
                .isInstanceOf(TrinoException.class)
                .hasMessage("message");
        verify(readErrorHandler, times(1))
                .handle(any(TrinoException.class), eq(rowGroupData), any(QueryContext.class));
    }

    @Test
    public void testNullValueOnProxied()
    {
        long[] warpMatches = new long[] {4, 5};
        LongArrayBlockBuilder proxiedBlock = new LongArrayBlockBuilder(null, warpMatches.length);
        proxiedBlock.appendNull();
        proxiedBlock.writeLong(99);

        Page firstProxiedPage = new Page(proxiedBlock.build());
        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, createRowRanges(0, 2));

        prepareMock(
                Lists.newArrayList(testPage),
                Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource mixQueryWithPredicate = getDispatcherPageSource(2, Map.of(1, BIGINT));
        Page resultPage = mixQueryWithPredicate.getNextPage(); //act
        int positionCount = resultPage.getPositionCount();
        assertThat(positionCount).isEqualTo(warpMatches.length);
        assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(0), 0)).isEqualTo(warpMatches[0]);
        assertThat(resultPage.getBlock(1).isNull(0)).isTrue();
        assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(0), 1)).isEqualTo(warpMatches[1]);
        assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(1), 1)).isEqualTo(99);
    }

    @Test
    public void mixedQuery_1ProxiedPages_singleRange()
    {
        RowRanges warpMatchRanges = createRowRanges(0, 1);

        long[] warpMatches = new long[] {4};
        long[] proxiedMatches = new long[] {99};

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
        if (assertNextPageMetrics) {
            assertThat(stats.getproxied_loaded_pages()).isEqualTo(1);
            assertThat(stats.getproxied_pages()).isEqualTo(1);
        }
    }

    /**
     * SIC-2235 protect in case proxied return empty page source
     */
    @Test
    public void mixedQuery_1ProxiedPages_singleRangeProxiedReturnEmptyPage()
    {
        RowRanges warpMatchRanges = createRowRanges(0, 65536);

        long[] warpMatches = new long[] {4};
        long[] proxiedMatches = new long[] {99};

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1);
        LongArrayBlockBuilder block = new LongArrayBlockBuilder(null, 0);
        Page emptyPage = new Page(block.build());
        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);
        prepareMock(
                warpPages,
                Lists.newArrayList(emptyPage, firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
        if (assertNextPageMetrics) {
            assertThat(stats.getproxied_loaded_pages()).isEqualTo(1);
            assertThat(stats.getproxied_pages()).isEqualTo(2);
        }
    }

    @Test
    public void mixedQuery_2ProxiedPages_singleRange()
    {
        RowRanges warpMatchRanges = createRowRanges(1, 2);

        long[] warpMatches = new long[] {4};
        long[] proxiedMatches = new long[] {99};

        Page firstProxiedPage = buildLongPage(-1);
        Page secondProxiedPage = buildLongPage(proxiedMatches[0], -1);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
        if (assertNextPageMetrics) {
            assertThat(stats.getproxied_loaded_pages()).isEqualTo(1);
            assertThat(stats.getproxied_pages()).isEqualTo(2);
        }
    }

    @Test
    public void test_skipAtEndOfWarpPage()
    {
        long[] warpMatches = new long[] {2, 5, 89};
        long[] proxiedMatches = new long[] {17, 99, 101};
        RowRanges warpMatchRanges1 = createRowRanges(0, 1);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1, proxiedMatches[1], -1, proxiedMatches[2], -1);

        Page warpPage = buildPageLong(Arrays.copyOf(warpMatches, 1));
        TestPage testPage1 = new TestPage(warpPage, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(2, 3, 4, 5);
        Page warpPage2 = buildPageLong(Arrays.copyOfRange(warpMatches, 1, warpMatches.length));
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void mixedQuery_2ProxiedPages_singleRange_lastRowMatch()
    {
        long[] warpMatches = new long[] {4};
        long[] proxiedMatches = new long[] {99};

        RowRanges warpMatchRanges = createRowRanges(2, 3);
        Page firstProxiedPage = buildLongPage(-1);
        Page secondProxiedPage = buildLongPage(-1, proxiedMatches[0]);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);

        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
        if (assertNextPageMetrics) {
            assertThat(stats.getproxied_loaded_pages()).isEqualTo(1);
            assertThat(stats.getproxied_pages()).isEqualTo(2);
        }
    }

    @Test
    public void mixedQuery_2ProxiedPages_overlappingRange()
    {
        long[] warpMatches = new long[] {2, 5, 89};
        long[] proxiedMatches = new long[] {17, 99, 101};
        RowRanges warpMatchRanges = createRowRanges(0, 2, 3, 4);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(proxiedMatches[1], -1, proxiedMatches[2]);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
        if (assertNextPageMetrics) {
            assertThat(stats.getproxied_loaded_pages()).isEqualTo(2);
            assertThat(stats.getproxied_pages()).isEqualTo(2);
        }
    }

    @Test
    public void test_2WarpPages_1ProxiedPage_MatchAtStartOfSecondWarpPage()
    {
        long[] warpMatches = new long[] {2, 9};
        long[] proxiedMatches = new long[] {88, 66};
        RowRanges warpMatchRanges1 = createRowRanges(0, 1);

        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 1);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(1, 2);

        pageValues = Arrays.copyOfRange(warpMatches, 1, 2);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], proxiedMatches[1], -1, -1, -1);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_singleProxiedPage_2WarpPages()
    {
        long[] warpMatches = new long[] {2, 9, 56, 77, 3};
        long[] proxiedMatches = new long[] {88, 66, 34, 54, 23};

        RowRanges warpMatchRanges1 = createRowRanges(1, 3);
        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 2);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(3, 6);

        pageValues = Arrays.copyOfRange(warpMatches, 2, 5);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        Page firstProxiedPage = buildLongPage(-1, proxiedMatches[0], proxiedMatches[1], proxiedMatches[2], proxiedMatches[3], proxiedMatches[4], -1);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_skipEntireProxiedPages()
    {
        int firstSkippedRows = 10000;
        long[] warpMatches = new long[] {2, 9, 56, 77, 3};
        long[] proxiedMatches = new long[] {88, 66, 34, 54, 23};

        RowRanges warpMatchRanges1 = createRowRanges(firstSkippedRows + 1, firstSkippedRows + 3);

        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 2);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(firstSkippedRows + 3, firstSkippedRows + 6);

        pageValues = Arrays.copyOfRange(warpMatches, 2, 5);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        long[] proxiedFirstPage = new long[firstSkippedRows];
        Arrays.fill(proxiedFirstPage, -1);
        Page firstProxiedPage = buildLongPage(proxiedFirstPage);
        Page secondProxiedPage = buildLongPage(-1, proxiedMatches[0], proxiedMatches[1], proxiedMatches[2], proxiedMatches[3], proxiedMatches[4], -1);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_2WarpPages_1ProxiedPage_MatchAtMiddleOfSecondWarpPage()
    {
        long[] warpMatches = new long[] {2, 9};
        long[] proxiedMatches = new long[] {88, 66};
        RowRanges warpMatchRanges1 = createRowRanges(0, 1);

        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 1);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(4, 5);

        pageValues = Arrays.copyOfRange(warpMatches, 1, 2);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1, -1, -1, proxiedMatches[1], -1);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_2WarpPages_1ProxiedPage_MatchAtMiddleOfSecondWarpPage2()
    {
        long[] warpMatches = new long[] {2, 9, 7};
        long[] proxiedMatches = new long[] {88, 66, 6};
        RowRanges warpMatchRanges1 = createRowRanges(0, 1);

        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 1);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(4, 5);

        pageValues = Arrays.copyOfRange(warpMatches, 1, 2);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        RowRanges warpMatchRanges3 = createRowRanges(8, 9);

        pageValues = Arrays.copyOfRange(warpMatches, 2, 3);
        Page warpPage3 = buildPageLong(pageValues);
        TestPage testPage3 = new TestPage(warpPage3, warpMatchRanges3);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1, -1, -1, proxiedMatches[1], -1);
        Page secondProxiedPage = buildLongPage(-1, -1, proxiedMatches[2]);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2, testPage3);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));
        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void testMultipleRanges()
    {
        long[] warpMatches = new long[] {1, 2, 3, 4, 5, 6, 7, 8};
        long[] proxiedMatches = new long[] {11, 22, 33, 44, 55, 66, 77, 88};

        Page firstProxiedPage = buildLongPage(-1, proxiedMatches[0], -1, -1, proxiedMatches[1], proxiedMatches[2], proxiedMatches[3], proxiedMatches[4], -1, -1, proxiedMatches[5], -1, proxiedMatches[6], -1, proxiedMatches[7]);
        RowRanges warpMatchRanges = createRowRanges(1, 2, 4, 8, 10, 11, 12, 13, 14, 15);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage1 = new TestPage(warpPage, warpMatchRanges);

        List<TestPage> warpPages = Lists.newArrayList(testPage1);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_2WarpPages_1ProxiedPage_MatchAtEndOfSecondWarpPage()
    {
        long[] warpMatches = new long[] {2, 9};
        long[] proxiedMatches = new long[] {88, 66};
        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 1);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, createRowRanges(0, 1));

        RowRanges warpMatchRanges = createRowRanges(4, 5);

        pageValues = Arrays.copyOfRange(warpMatches, 1, 2);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1, -1, -1, proxiedMatches[1]);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_complex()
    {
        long[] warpMatches = new long[10];
        long[] proxiedMatches = new long[10];
        for (int i = 0; i < 10; i++) {
            warpMatches[i] = i;
            proxiedMatches[i] = i + 10L;
        }
        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 5);
        Page warpPage1 = buildPageLong(pageValues);
        RowRanges warpMatchRanges1 = createRowRanges(1, 3, 5, 7, 9, 10);

        Page firstProxiedPage = buildLongPage(-1, proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(proxiedMatches[1], -1);
        Page thirdProxiedPage = buildLongPage(-1, proxiedMatches[2], proxiedMatches[3]);
        Page fiveProxiedPage = buildLongPage(-1, -1, proxiedMatches[4]);
        TestPage testPage1 = new TestPage(warpPage1, warpMatchRanges1);

        long[] pageValues2 = Arrays.copyOfRange(warpMatches, 5, 10);
        Page warpPage2 = buildPageLong(pageValues2);
        RowRanges warpMatchRanges2 = createRowRanges(10, 12, 14, 16, 18, 19);
        Page sixProxiedPage = buildLongPage(proxiedMatches[5]);
        Page sevenProxiedPage = buildLongPage(proxiedMatches[6], -1);
        Page eightProxiedPage = buildLongPage(-1, proxiedMatches[7], proxiedMatches[8]);
        Page nineProxiedPage = buildLongPage(-1, -1, proxiedMatches[9]);
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage, thirdProxiedPage, fiveProxiedPage,
                        sixProxiedPage, sevenProxiedPage, eightProxiedPage, nineProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_2WarpPages_2ProxiedPage_NoSkip()
    {
        long[] warpMatches = new long[] {2, 9};
        long[] proxiedMatches = new long[] {88, 66};
        long[] pageValues = Arrays.copyOfRange(warpMatches, 0, 1);
        Page warpPage1 = buildPageLong(pageValues);
        TestPage testPage1 = new TestPage(warpPage1, createRowRanges(1, 2));

        pageValues = Arrays.copyOfRange(warpMatches, 1, 2);
        Page warpPage2 = buildPageLong(pageValues);
        TestPage testPage2 = new TestPage(warpPage2, createRowRanges(3, 4));

        Page firstProxiedPage = buildLongPage(-1, proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(-1, proxiedMatches[1]);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        actAndAssert(warpMatches, proxiedMatches, dispatcherPageSource);
    }

    @Test
    public void test_1WarpPage_1prefilled()
    {
        long[] warpMatches = new long[] {2, 9};
        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, createRowRanges(1, 2));
        List<TestPage> warpPages = Lists.newArrayList(testPage);

        long singleValue = 5L;
        Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = Map.of(1, createPrefilledQueryCollectData(singleValue));
        prepareMock(warpPages, List.of());

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, Map.of(), prefilledQueryCollectDataByBlockIndex);

        actAndAssert(warpMatches, Optional.empty(), dispatcherPageSource, Optional.of(singleValue));
    }

    @Test
    public void test_2WarpPages_2ProxiedPage_1prefilled_NoSkip()
    {
        long[] warpMatches = new long[] {2, 5, 89};
        long[] proxiedMatches = new long[] {17, 99, 101};
        RowRanges warpMatchRanges1 = createRowRanges(0, 1);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0], -1, proxiedMatches[1], -1, proxiedMatches[2], -1);

        Page warpPage = buildPageLong(Arrays.copyOf(warpMatches, 1));
        TestPage testPage1 = new TestPage(warpPage, warpMatchRanges1);

        RowRanges warpMatchRanges2 = createRowRanges(2, 3, 4, 5);
        Page warpPage2 = buildPageLong(Arrays.copyOfRange(warpMatches, 1, warpMatches.length));
        TestPage testPage2 = new TestPage(warpPage2, warpMatchRanges2);

        List<TestPage> warpPages = Lists.newArrayList(testPage1, testPage2);

        long singleValue = 3L;
        Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = Map.of(2, createPrefilledQueryCollectData(singleValue));
        prepareMock(warpPages, Lists.newArrayList(firstProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(3, proxiedCollectTypeByBlockIndex, prefilledQueryCollectDataByBlockIndex);

        actAndAssert(warpMatches, Optional.of(proxiedMatches), dispatcherPageSource, Optional.of(singleValue));
    }

    @Test
    public void mixedQuery_2ProxiedPages_overlappingRange_with_prefilled()
    {
        long[] warpMatches = new long[] {2, 5, 89};
        long[] proxiedMatches = new long[] {17, 99, 101};
        RowRanges warpMatchRanges = createRowRanges(0, 2, 3, 4);

        Page firstProxiedPage = buildLongPage(proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(proxiedMatches[1], -1, proxiedMatches[2]);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);

        long singleValue = 3L;
        Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = Map.of(2, createPrefilledQueryCollectData(singleValue));
        prepareMock(warpPages, Lists.newArrayList(firstProxiedPage, secondProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(3, proxiedCollectTypeByBlockIndex, prefilledQueryCollectDataByBlockIndex);

        actAndAssert(warpMatches, Optional.of(proxiedMatches), dispatcherPageSource, Optional.of(singleValue));
    }

    /**
     * simulate query "select * from t limit 2":
     * native are not aware of 'limit' section, so it returns maximum rows fit into page.
     * in this case sumRowRanges > warpPageCount
     */
    @Test
    public void mixedQuery_2ProxiedPages_singleRange_simulateLimit()
    {
        RowRanges warpMatchRanges = createRowRanges(0, 3);

        long[] warpMatches = new long[] {4, 5};
        long[] proxiedMatches = new long[] {99, 34, 99};

        Page firstProxiedPage = buildLongPage(proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(proxiedMatches[1], proxiedMatches[2]);

        Page warpPage = buildPageLong(warpMatches);
        TestPage testPage = new TestPage(warpPage, warpMatchRanges);
        List<TestPage> warpPages = Lists.newArrayList(testPage);
        prepareMock(
                warpPages,
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);

        long[] expectedProxiedMatches = Arrays.copyOfRange(proxiedMatches, 0, 2);
        actAndAssert(warpMatches, expectedProxiedMatches, dispatcherPageSource);
    }

    @Test
    public void mixedQueryEmptyWarpPageSource()
    {
        long[] proxiedMatches = new long[] {99, 34, 99};
        Page firstProxiedPage = buildLongPage(proxiedMatches[0]);
        Page secondProxiedPage = buildLongPage(proxiedMatches[1], proxiedMatches[2]);

        prepareMock(
                List.of(),
                Lists.newArrayList(firstProxiedPage, secondProxiedPage));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);
        actAndAssert(new long[] {}, new long[] {}, dispatcherPageSource);
    }

    @Test
    public void mixedQueryProxiedRowRangesIntersection()
    {
        List<TestPage> warpPages = List.of(
                new TestPage(buildLongPage(100, 101), createRowRanges(0, 2)),
                new TestPage(buildLongPage(105, 106), createRowRanges(5, 7)),
                new TestPage(buildLongPage(110, 111), createRowRanges(11, 13)),
                new TestPage(buildLongPage(115, 116), createRowRanges(15, 17)));
        prepareMock(
                warpPages,
                List.of(buildLongPage(1, 2), buildLongPage(3, 4), buildLongPage(5, 6, 7, 8, 9, 10, 11, 12, 13)));

        DispatcherPageSource dispatcherPageSource = getDispatcherPageSource(2, proxiedCollectTypeByBlockIndex);
        actAndAssert(new long[] {100, 101, 105, 106, 110, 111}, new long[] {1, 2, 6, 7, 12, 13}, dispatcherPageSource);
    }

    private PrefilledQueryCollectData createPrefilledQueryCollectData(long singleValue)
    {
        return PrefilledQueryCollectData.builder()
                .warpColumn(new RegularColumn("col1"))
                .type(BIGINT)
                .blockIndex(0)
                .singleValue(SingleValue.create(BIGINT, singleValue))
                .build();
    }

    @Test
    public void testAddWithSmallPage()
    {
        assertNextPageMetrics = false;
        test_complex();
        testMultipleRanges();
        mixedQuery_2ProxiedPages_overlappingRange();
        test_2WarpPages_1ProxiedPage_MatchAtEndOfSecondWarpPage();
        test_2WarpPages_1ProxiedPage_MatchAtMiddleOfSecondWarpPage();
        test_2WarpPages_1ProxiedPage_MatchAtStartOfSecondWarpPage();
        test_singleProxiedPage_2WarpPages();
        mixedQuery_1ProxiedPages_singleRange();
        mixedQuery_2ProxiedPages_singleRange_lastRowMatch();
        test_2WarpPages_2ProxiedPage_NoSkip();
        test_2WarpPages_1ProxiedPage_MatchAtMiddleOfSecondWarpPage2();
        test_1WarpPage_1prefilled();
        test_2WarpPages_2ProxiedPage_1prefilled_NoSkip();
        mixedQuery_2ProxiedPages_overlappingRange_with_prefilled();
        mixedQuery_2ProxiedPages_singleRange_simulateLimit();
    }

    private DispatcherPageSource getDispatcherPageSource(int totalCollectColumns, Map<Integer, Type> proxiedCollectTypeByBlockIndex)
    {
        return getDispatcherPageSource(totalCollectColumns, proxiedCollectTypeByBlockIndex, Collections.emptyMap());
    }

    // NativeQueryCollectData is created for each block index that isn't in proxied \ prefilled maps
    private DispatcherPageSource getDispatcherPageSource(int totalCollectColumns,
            Map<Integer, Type> proxiedCollectTypeByBlockIndex,
            Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex)
    {
        Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>();
        List<NativeQueryCollectData> nativeQueryCollectDataList = new ArrayList<>();

        for (int i = 0; i < totalCollectColumns; i++) {
            if (proxiedCollectTypeByBlockIndex.containsKey(i)) {
                TestingConnectorColumnHandle columnHandle = mock(TestingConnectorColumnHandle.class);
                when(dispatcherProxiedConnectorTransformer.getColumnType(eq(columnHandle))).thenReturn(proxiedCollectTypeByBlockIndex.get(i));
                remainingCollectColumnByBlockIndex.put(i, columnHandle);
            }
            else {
                if (!prefilledQueryCollectDataByBlockIndex.containsKey(i)) {
                    WarmUpElement warmUpElement = WarmUpElement.builder()
                            .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                            .recTypeCode(RecTypeCode.REC_TYPE_BIGINT)
                            .recTypeLength(8)
                            .colName("colName")
                            .totalRecords(5)
                            .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                            .build();
                    nativeQueryCollectDataList.add(NativeQueryCollectData.builder()
                            .warmUpElement(warmUpElement)
                            .blockIndex(i)
                            .type(BIGINT)
                            .build());
                }
            }
        }

        QueryContext queryContext = new QueryContext(new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.TRUE),
                ImmutableMap.copyOf(remainingCollectColumnByBlockIndex), "query-id")
                .asBuilder()
                .nativeQueryCollectDataList(nativeQueryCollectDataList)
                .prefilledQueryCollectDataByBlockIndex(prefilledQueryCollectDataByBlockIndex)
                .build();

        List<Type> warpWithoutPrefilledAndProxiedCollectTypes = Stream.concat(
                        queryContext.getRemainingCollectColumns().stream().map(dispatcherProxiedConnectorTransformer::getColumnType),
                        queryContext.getNativeQueryCollectDataList().stream().map(QueryColumn::getType))
                .collect(toImmutableList());

        return new DispatcherPageSource(() -> proxiedConnectorPageSource,
                mock(QueryClassifier.class),
                warpWithoutPrefilledAndProxiedCollectTypes,
                warpPageSource,
                queryContext,
                rowGroupData,
                proxiedCollectTypeByBlockIndex.isEmpty() ? PageSourceDecision.WARP : PageSourceDecision.MIXED,
                stats,
                closeHandler,
                null,
                null,
                0,
                readErrorHandler,
                new GlobalConfig());
    }

    private Page buildPageLong(long[] values)
    {
        PageBuilder warpPageBuilder = new PageBuilder(List.of(BigintType.BIGINT));
        BlockBuilder warpBlockBuilder = warpPageBuilder.getBlockBuilder(0);
        Page page = buildLongPage(values);
        Block block = page.getBlock(0);
        for (int i = 0; i < values.length; i++) {
            ((Type) io.trino.spi.type.BigintType.BIGINT).appendTo(block, i, warpBlockBuilder);
        }
        warpPageBuilder.declarePositions(values.length);
        return warpPageBuilder.build();
    }

    private void actAndAssert(long[] warpMatches, long[] proxiedMatches, DispatcherPageSource dispatcherPageSource)
    {
        actAndAssert(warpMatches, Optional.of(proxiedMatches), dispatcherPageSource, Optional.empty());
    }

    private void actAndAssert(long[] warpMatches,
            Optional<long[]> proxiedMatches,
            DispatcherPageSource dispatcherPageSource,
            Optional<Long> prefilled)
    {
        int totalMatchesPosition = 0;
        while (!dispatcherPageSource.isFinished()) {
            Page resultPage = dispatcherPageSource.getNextPage(); //act
            int positionCount = resultPage.getPositionCount();
            for (int i = 0; i < positionCount; i++) {
                assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(0), i)).isEqualTo(warpMatches[totalMatchesPosition + i]);
                if (proxiedMatches.isPresent()) {
                    assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(1), i)).isEqualTo(proxiedMatches.orElseThrow()[totalMatchesPosition + i]);
                }
                if (prefilled.isPresent()) {
                    int channel = proxiedMatches.isPresent() ? 2 : 1;
                    assertThat(BigintType.BIGINT.getLong(resultPage.getBlock(channel), i)).isEqualTo(prefilled.orElseThrow());
                }
            }
            totalMatchesPosition += positionCount;
        }

        if (proxiedMatches.isPresent()) {
            assertThat(totalMatchesPosition).isEqualTo(proxiedMatches.orElseThrow().length);
        }
        assertThat(totalMatchesPosition).isEqualTo(warpMatches.length);
    }

    private static RowRanges createRowRanges(long... ranges)
    {
        long[] lowInclusive = new long[ranges.length / 2];
        long[] upperExclusive = new long[ranges.length / 2];
        for (int i = 0; i < ranges.length / 2; i++) {
            lowInclusive[i] = ranges[i * 2];
            upperExclusive[i] = ranges[i * 2 + 1];
        }
        return new RowRanges(lowInclusive, upperExclusive, false);
    }

    public static class TestPage
    {
        private final Page page;
        private final RowRanges matchRanges;

        public TestPage(Page page, RowRanges matchRanges)
        {
            this.page = page;
            this.matchRanges = matchRanges;
        }

        public Page getPage()
        {
            return page;
        }

        public RowRanges getMatchedRanges()
        {
            return matchRanges;
        }
    }
}
