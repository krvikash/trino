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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSinkDataPool
{
    @Test
    public void testPoolsFromEmpty()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 2);
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2))).isEmpty();
    }

    @Test
    public void testPoolsOnlyFromTracked()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 2);
        // add data for 3
        dataPool.add(3, utf8Slice("data_for_3_0"));
        // poll for 1 and 2
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2))).isEmpty();
    }

    @Test
    public void testPoolsUpToTargetPagesCount()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2"));
        dataPool.add(1, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        dataPool.add(2, utf8Slice("page5"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));
    }

    @Test
    public void testPoolsUpToTargetPagesSize()
    {
        SinkDataPool dataPool = newDataPool(8, DataSize.of(16, BYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2x")); // make value longer so partition 1 has higher priority than partition 2
        dataPool.add(2, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2x"),
                2, utf8Slice("page3")));
    }

    @Test
    public void testPoolsUpToTargetPartitionsCount()
    {
        SinkDataPool dataPool = newDataPool(8, DataSize.of(8, MEGABYTE), 2);
        // add data for 3
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(2, utf8Slice("page2x"));
        dataPool.add(3, utf8Slice("page3xy"));
        // poll for 1 and 2
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
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
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));

        // rollback and poll again
        pollResult.get().rollback();
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getDataByPartition()).isEqualTo(ImmutableListMultimap.of(
                1, utf8Slice("page1"),
                1, utf8Slice("page2"),
                1, utf8Slice("page3"),
                2, utf8Slice("page4")));

        // commit and poll again
        pollResult.get().commit();
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
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
        dataPool.pollBest(ImmutableSet.of(1)).orElseThrow().commit();
        assertThat(dataPool.whenFinished()).isDone();

        // non-empty pool; pollBest called first
        dataPool = newDataPool(32, DataSize.of(8, MEGABYTE), 8);
        dataPool.add(1, utf8Slice("data"));
        dataPool.pollBest(ImmutableSet.of(1)).orElseThrow().commit();
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
                32,
                DataSize.of(8, MEGABYTE),
                8);

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
        dataPool.pollBest(ImmutableSet.of(1)).orElseThrow().commit();
        assertThat(dataPool.isBlocked()).isSameAs(returnedIsBlocked);
        assertThat(returnedIsBlocked).isNotDone();

        // below low
        dataPool.pollBest(ImmutableSet.of(2)).orElseThrow().commit();
        assertThat(dataPool.isBlocked()).isDone();
        assertThat(returnedIsBlocked).isDone();
    }

    @Test
    public void testPoolsBiggestPartitions()
    {
        SinkDataPool dataPool = newDataPool(4, DataSize.of(8, MEGABYTE), 4);
        dataPool.add(1, utf8Slice("page1"));
        dataPool.add(1, utf8Slice("page2"));
        dataPool.add(1, utf8Slice("page3"));
        dataPool.add(2, utf8Slice("page4"));
        dataPool.add(2, utf8Slice("page5_is_bigger_than_others"));

        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
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

    private static SinkDataPool newDataPool(int targetWrittenPagesCount, DataSize targetWrittenPagesSize, int targetWrittenPartitionsCount)
    {
        return new SinkDataPool(
                DataSize.of(1, MEGABYTE),
                DataSize.of(4, MEGABYTE),
                targetWrittenPagesCount,
                targetWrittenPagesSize,
                targetWrittenPartitionsCount);
    }
}
