/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSinkDataPool
{
    @Test
    public void testPollsFromEmpty()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 2);
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2), false)).isEmpty();
    }

    @Test
    public void testPollsOnlyFromTracked()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 2);
        // add data for 3
        dataPool.add(3, utf8Slice("data_for_3_0"));
        // poll for 1 and 2
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2), false)).isEmpty();
    }

    @Test
    public void testPollsUpToTargetPagesCount()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2"));
        dataPool.add(1, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        dataPool.add(2, utf8Slice("page5"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));
    }

    @Test
    public void testPollsUpToTargetPagesSize()
    {
        SinkDataPool dataPool = newDataPool(8, DataSize.of(16, BYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2x")); // make value longer so partition 1 has higher priority than partition 2
        dataPool.add(2, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2x"),
                2, utf8Slice("page3")));
    }

    @Test
    public void testPollsUpToTargetPartitionsCount()
    {
        SinkDataPool dataPool = newDataPool(8, DataSize.of(8, MEGABYTE), 2);
        // add data for 3
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(2, utf8Slice("page2x"));
        dataPool.add(3, utf8Slice("page3xy"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                2, utf8Slice("page2x")));
    }

    @Test
    public void testRollbackCommit()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2"));
        dataPool.add(1, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        dataPool.add(2, utf8Slice("page5"));

        Optional<SinkDataPool.PollResult> pollResult;

        // poll
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));

        // rollback and poll again
        pollResult.get().rollback();
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));

        // commit and poll again
        pollResult.get().commit();
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                2, utf8Slice("page5")));
    }

    @Test
    public void testWhenFinished()
    {
        // test empty pool
        SinkDataPool dataPool = newDataPool(32, DataSize.of(8, MEGABYTE), 8);
        assertThat(dataPool.whenFinished()).isNotDone();
        dataPool.noMoreData();
        assertThat(dataPool.whenFinished()).isDone();

        // non-empty pool; noMoreData called
        dataPool = newDataPool(32, DataSize.of(8, MEGABYTE), 8);
        dataPool.add(1, utf8Slice("data"));
        dataPool.noMoreData();
        assertThat(dataPool.whenFinished()).isNotDone();
        dataPool.pollBest(ImmutableSet.of(1), false).orElseThrow().commit();
        assertThat(dataPool.whenFinished()).isDone();

        // non-empty pool; pollBest called first
        dataPool = newDataPool(32, DataSize.of(8, MEGABYTE), 8);
        dataPool.add(1, utf8Slice("data"));
        dataPool.pollBest(ImmutableSet.of(1), false).orElseThrow().commit();
        assertThat(dataPool.whenFinished()).isNotDone();
        dataPool.noMoreData();
        assertThat(dataPool.whenFinished()).isDone();
    }

    @Test
    public void testIsBlocked()
    {
        SinkDataPool dataPool = new SinkDataPool(
                DataSize.of(100, BYTE),
                DataSize.of(200, BYTE),
                succinctDuration(0, SECONDS),
                0,
                DataSize.of(0, MEGABYTE),
                32,
                DataSize.of(8, MEGABYTE),
                8,
                Ticker.systemTicker());

        // 0
        assertThat(dataPool.isBlocked()).isDone();

        // below low
        dataPool.add(1, utf8Slice("123456789012345"));
        assertThat(dataPool.isBlocked()).isDone();

        // above low but below high
        dataPool.add(2, utf8Slice("123456789012345"));
        assertThat(dataPool.isBlocked()).isDone();

        // above high
        dataPool.add(3, utf8Slice("123456789012345"));
        ListenableFuture<Void> returnedIsBlocked = dataPool.isBlocked();
        assertThat(returnedIsBlocked).isNotDone();

        // below high but still above low
        dataPool.pollBest(ImmutableSet.of(1), false).orElseThrow().commit();
        assertThat(dataPool.isBlocked()).isSameAs(returnedIsBlocked);
        assertThat(returnedIsBlocked).isNotDone();

        // below low
        dataPool.pollBest(ImmutableSet.of(2), false).orElseThrow().commit();
        assertThat(dataPool.isBlocked()).isDone();
        assertThat(returnedIsBlocked).isDone();
    }

    @Test
    public void testPollsBiggestPartitions()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2"));
        dataPool.add(1, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        dataPool.add(2, utf8Slice("page5_is_bigger_than_others"));

        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2), false);
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                2, utf8Slice("page4"),
                2, utf8Slice("page5_is_bigger_than_others"),
                1, utf8Slice("page1"),
                1, utf8Slice("page2")));
    }

    @Test
    public void testAddEmptyDataPage()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        assertThatThrownBy(() -> dataPool.add(1, utf8Slice("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cannot add empty data page");
    }

    @Test
    public void testPollMinCountMinSizeMaxWait()
    {
        TestingTicker ticker = new TestingTicker();

        SinkDataPool dataPool = new SinkDataPool(
                DataSize.of(100, BYTE),
                DataSize.of(200, BYTE),
                succinctDuration(1, SECONDS),
                2,
                DataSize.of(15, BYTE),
                32,
                DataSize.of(8, MEGABYTE),
                8,
                ticker);

        // not enough count, not enough size, not enough time
        dataPool.add(1, utf8Slice("page1"));
        Optional<SinkDataPool.PollResult> pollResult1 = dataPool.pollBest(ImmutableSet.of(1), false);
        assertThat(pollResult1).isEmpty();

        // not enough count, not enough size, but enough time
        ticker.increment(2, SECONDS);
        Optional<SinkDataPool.PollResult> pollResult2 = dataPool.pollBest(ImmutableSet.of(1), false);
        assertThat(pollResult2).isPresent();
        pollResult2.get().commit();
        assertThat(pollResult2.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1")));

        // not enough count, not enough size, not enough time
        dataPool.add(2, utf8Slice("page2"));
        Optional<SinkDataPool.PollResult> pollResult3 = dataPool.pollBest(ImmutableSet.of(2), false);
        assertThat(pollResult3).isEmpty();

        // enough count, not enough size, not enough time
        dataPool.add(3, utf8Slice("page3"));
        Optional<SinkDataPool.PollResult> pollResult4 = dataPool.pollBest(ImmutableSet.of(2, 3), false);
        assertThat(pollResult4).isPresent();
        assertThat(pollResult4.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                2, utf8Slice("page2"),
                3, utf8Slice("page3")));
        pollResult4.get().commit();

        // not enough count, enough size, not enough time
        dataPool.add(4, utf8Slice("page4page4page4"));
        Optional<SinkDataPool.PollResult> pollResult5 = dataPool.pollBest(ImmutableSet.of(4), false);
        assertThat(pollResult5).isPresent();
        assertThat(pollResult5.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                4, utf8Slice("page4page4page4")));
        pollResult5.get().commit();

        // not enough count, not enough size, not enough time, but finishing
        dataPool.add(5, utf8Slice("page5"));
        Optional<SinkDataPool.PollResult> pollResult6 = dataPool.pollBest(ImmutableSet.of(5), true);
        assertThat(pollResult6).isPresent();
        assertThat(pollResult6.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                5, utf8Slice("page5")));
        pollResult6.get().commit();

        // not enough count, not enough size, not enough time, but dataPool is blocked
        dataPool.add(6, utf8Slice("page6"));
        dataPool.add(7, utf8Slice("7".repeat(200)));
        assertThat(dataPool.isBlocked()).isNotDone();
        Optional<SinkDataPool.PollResult> pollResult7 = dataPool.pollBest(ImmutableSet.of(6), false);
        assertThat(pollResult7).isPresent();
        assertThat(pollResult7.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                6, utf8Slice("page6")));
        pollResult7.get().commit();
    }

    private static SinkDataPool newDataPool(int targetWrittenPagesCount, DataSize targetWrittenPagesSize, int targetWrittenPartitionsCount)
    {
        return new SinkDataPool(
                DataSize.of(1, MEGABYTE),
                DataSize.of(4, MEGABYTE),
                succinctDuration(0, SECONDS),
                0,
                DataSize.of(0, MEGABYTE),
                targetWrittenPagesCount,
                targetWrittenPagesSize,
                targetWrittenPartitionsCount,
                Ticker.systemTicker());
    }
}
