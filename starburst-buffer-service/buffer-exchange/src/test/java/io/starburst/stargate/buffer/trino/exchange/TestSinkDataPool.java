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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
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
    public void testHappy()
    {
        SinkDataPool dataPool = newDataPool();

        // empty pool
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2))).isEmpty();

        // add data for 3
        dataPool.add(3, utf8Slice("data_for_3_0"));

        // still no data for 1 or 2
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2))).isEmpty();

        // add data for 2
        dataPool.add(2, utf8Slice("data_for_2_0"));

        // poll
        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getPartition()).isEqualTo(2);
        assertThat(pollResult.get().getData()).containsExactly(utf8Slice("data_for_2_0"));

        // rollback
        pollResult.get().rollback();

        // poll again
        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getPartition()).isEqualTo(2);
        assertThat(pollResult.get().getData()).containsExactly(utf8Slice("data_for_2_0"));

        // commit
        pollResult.get().commit();

        // no more data
        assertThat(dataPool.pollBest(ImmutableSet.of(1, 2))).isEmpty();

        // not done yet - data for partition 3 and noMoreDataFlag not set
        assertThat(dataPool.whenFinished()).isNotDone();

        // clear partition 3
        pollResult = dataPool.pollBest(ImmutableSet.of(3));
        assertThat(pollResult).isNotEmpty();
        pollResult.get().commit();

        // not done yet -  noMoreDataFlag not set
        assertThat(dataPool.whenFinished()).isNotDone();

        // mark noMoreData
        dataPool.noMoreData();

        // not done yet -  noMoreDataFlag not set
        assertThat(dataPool.whenFinished()).isDone();
    }

    @Test
    public void testWhenFinished()
    {
        // test empty pool
        SinkDataPool dataPool = newDataPool();
        assertThat(dataPool.whenFinished()).isNotDone();
        dataPool.noMoreData();
        assertThat(dataPool.whenFinished()).isDone();

        // non-empty pool; noMoreData called
        dataPool = newDataPool();
        dataPool.add(1, utf8Slice("data"));
        dataPool.noMoreData();
        dataPool.pollBest(ImmutableSet.of(1)).orElseThrow().commit();
        assertThat(dataPool.whenFinished()).isDone();

        // non-empty pool; pollBest called first
        dataPool = newDataPool();
        dataPool.add(1, utf8Slice("data"));
        dataPool.pollBest(ImmutableSet.of(1)).orElseThrow().commit();
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
                DataSize.of(8, MEGABYTE));

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
    public void testPollBestTakesBiggest()
    {
        SinkDataPool dataPool = newDataPool();
        dataPool.add(1, utf8Slice("1_1234567890"));
        dataPool.add(2, utf8Slice("1_123456789012345"));
        dataPool.add(3, utf8Slice("1_123456789012"));

        Optional<SinkDataPool.PollResult> pollResult = dataPool.pollBest(ImmutableSet.of(1, 2, 3));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getPartition()).isEqualTo(2);
        assertThat(pollResult.get().getData()).containsExactly(utf8Slice("1_123456789012345"));

        dataPool = newDataPool();
        dataPool.add(1, utf8Slice("1_1234567890"));
        dataPool.add(2, utf8Slice("1_1234567_1"));
        dataPool.add(2, utf8Slice("1_1234567_2"));
        dataPool.add(3, utf8Slice("1_123456789012"));

        pollResult = dataPool.pollBest(ImmutableSet.of(1, 2, 3));
        assertThat(pollResult).isPresent();
        assertThat(pollResult.get().getPartition()).isEqualTo(2);
        assertThat(pollResult.get().getData()).containsExactly(utf8Slice("1_1234567_1"), utf8Slice("1_1234567_2"));
    }

    @Test
    public void testAddEmptyDataPage()
    {
        SinkDataPool dataPool = newDataPool();
        assertThatThrownBy(() -> dataPool.add(1, utf8Slice("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cannot add empty data page");
    }

    private static SinkDataPool newDataPool()
    {
        return new SinkDataPool(
                DataSize.of(1, MEGABYTE),
                DataSize.of(4, MEGABYTE),
                32,
                DataSize.of(8, MEGABYTE));
    }
}
