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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;

class TestSinkMappingScaler
{
    @Test
    public void testInitializeWithBaseMapping()
    {
        TestingTicker ticker = new TestingTicker();
        PartitionNodeMapping fullMapping = new PartitionNodeMapping(
                ImmutableListMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L, 4L, 5L, 6L, 7L)
                        .putAll(1, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.<Integer, Integer>builder()
                        .put(0, 2)
                        .put(1, 3)
                        .put(2, 2)
                        .buildOrThrow());

        SinkMappingScaler scaler = createScaler(fullMapping, 1.5, 5.0, ticker);

        SinkMappingScaler.SinkStateProvider state;
        state = sinkState(ImmutableMultimap.of(), ImmutableMap.of(), Optional.empty());
        twice(() -> {
            SinkMappingScaler.Result result = scaler.process(state);
            assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(1L, 2L);
            assertThat(result.writersToAdd().get(1)).containsExactlyInAnyOrder(11L, 12L, 13L);
            assertThat(result.writersToAdd().get(2)).containsExactlyInAnyOrder(21L, 22L);
        });
    }

    @Test
    public void testExtendsWithBaseMapping()
    {
        TestingTicker ticker = new TestingTicker();
        PartitionNodeMapping fullMapping = new PartitionNodeMapping(
                ImmutableListMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L, 4L, 5L, 6L, 7L)
                        .putAll(1, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.<Integer, Integer>builder()
                        .put(0, 2)
                        .put(1, 3)
                        .put(2, 2)
                        .buildOrThrow());

        SinkMappingScaler scaler = createScaler(fullMapping, 1.5, 5.0, ticker);

        SinkMappingScaler.SinkStateProvider state;
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L)
                        .putAll(1, 12L, 14L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.of(),
                Optional.empty());
        twice(() -> {
            SinkMappingScaler.Result result = scaler.process(state);
            assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0, 1);
            assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(2L);
            assertThat(result.writersToAdd().get(1)).containsExactlyInAnyOrder(11L, 13L);
        });
    }

    @Test
    public void testScaleUp()
    {
        TestingTicker ticker = new TestingTicker();
        PartitionNodeMapping fullMapping = new PartitionNodeMapping(
                ImmutableListMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L, 4L, 5L, 6L, 7L)
                        .putAll(1, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.<Integer, Integer>builder()
                        .put(0, 2)
                        .put(1, 3)
                        .put(2, 2)
                        .buildOrThrow());

        SinkMappingScaler scaler = createScaler(fullMapping, 1.5, 5.0, ticker);

        // initial scale-up with base extension
        SinkMappingScaler.SinkStateProvider state;
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L)
                        .putAll(1, 12L, 14L)
                        .putAll(2, 21L)
                        .build(),
                ImmutableMap.of(
                        0, 50L,
                        1, 50L,
                        2, 0L),
                Optional.of(succinctDuration(5.1, SECONDS)));
        SinkMappingScaler.Result result = scaler.process(state);
        assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(2L, 3L); // target size 3
        assertThat(result.writersToAdd().get(1)).containsExactlyInAnyOrder(11L, 13L); // target size 4
        assertThat(result.writersToAdd().get(2)).containsExactlyInAnyOrder(22L); // target size 2 (just fill missing base)

        // immediate processing does not trigger scaleup
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L)
                        .putAll(1, 11L, 12L, 13L, 14L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.of(
                        0, 50L,
                        1, 50L,
                        2, 0L),
                Optional.of(succinctDuration(5.1, SECONDS)));
        result = scaler.process(state);
        assertThat(result.writersToAdd()).isEmpty();

        // enough time passes since last scaleup but we do not get full data pool -> no scaleup
        ticker.increment(10, SECONDS);
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L)
                        .putAll(1, 11L, 12L, 13L, 14L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.of(
                        0, 50L,
                        1, 50L,
                        2, 0L),
                Optional.of(succinctDuration(15.1, SECONDS)));
        result = scaler.process(state);
        assertThat(result.writersToAdd()).isEmpty();

        // we get full pool but to quickly after last scaleup; it needs to be more than 5.0 * 0.3 after last scaleup
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L)
                        .putAll(1, 11L, 12L, 13L, 14L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.of(
                        0, 50L,
                        1, 50L,
                        2, 0L),
                Optional.of(succinctDuration(8.51, SECONDS)));
        result = scaler.process(state);
        assertThat(result.writersToAdd()).isEmpty();

        // last time pool is full is soon enough
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L)
                        .putAll(1, 11L, 12L, 13L, 14L)
                        .putAll(2, 21L, 22L)
                        .build(),
                ImmutableMap.of(
                        0, 50L,
                        1, 50L,
                        2, 0L),
                Optional.of(succinctDuration(8.49, SECONDS)));
        result = scaler.process(state);
        assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0);
        assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(4L); // target size 4 (limit)
    }

    @Test
    public void testChangeMapping()
    {
        TestingTicker ticker = new TestingTicker();
        PartitionNodeMapping fullMapping = new PartitionNodeMapping(
                ImmutableListMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L, 3L, 4L, 5L, 6L, 7L)
                        .putAll(1, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
                        .build(),
                ImmutableMap.<Integer, Integer>builder()
                        .put(0, 2)
                        .put(1, 2)
                        .buildOrThrow());

        SinkMappingScaler scaler = createScaler(fullMapping, 2.0, 5.0, ticker);

        // initial scale-up with old mapping
        SinkMappingScaler.SinkStateProvider state;
        state = sinkState(
                ImmutableMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 2L)
                        .putAll(1, 11L, 12L)
                        .build(),
                ImmutableMap.of(
                        0, 80L,
                        1, 20L),
                Optional.of(succinctDuration(5.1, SECONDS)));
        SinkMappingScaler.Result result = scaler.process(state);
        assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0);
        assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(3L, 4L); // target size 4

        PartitionNodeMapping newMapping = new PartitionNodeMapping(
                ImmutableListMultimap.<Integer, Long>builder()
                        .putAll(0, 1L, 5L, 8L, 9L, 3L)
                        .putAll(1, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L)
                        .build(),
                ImmutableMap.<Integer, Integer>builder()
                        .put(0, 2)
                        .put(1, 2)
                        .buildOrThrow());

        scaler.receivedUpdatedPartitionNodeMapping(newMapping);

        // for empty active mapping we should just get prefixes according to target size per partition
        result = scaler.process(sinkState(ImmutableMultimap.of(), ImmutableMap.of(), Optional.empty()));
        assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0, 1);
        assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(1L, 5L, 8L, 9L); // target size 3
        assertThat(result.writersToAdd().get(1)).containsExactlyInAnyOrder(11L, 12L); // target size 4

        // if some nodes are already in active mapping those will be reused
        result = scaler.process(sinkState(ImmutableMultimap.<Integer, Long>builder()
                .putAll(0, 1L, 3L)
                .putAll(1, 13L)
                .build(),
                ImmutableMap.of(),
                Optional.empty()));
        assertThat(result.writersToAdd().keySet()).containsExactlyInAnyOrder(0, 1);
        assertThat(result.writersToAdd().get(0)).containsExactlyInAnyOrder(5L, 8L); // we need to add 5L to have base mapping and then one more
        assertThat(result.writersToAdd().get(1)).containsExactlyInAnyOrder(11L, 12L); // we need add 11L and 12L to have base mapping covered
    }

    private void twice(Runnable runnable)
    {
        for (int i = 0; i < 2; i++) {
            runnable.run();
        }
    }

    private static SinkMappingScaler createScaler(PartitionNodeMapping fullMapping, double maxScaleUpGrowthFactor, double minTimeBetweenScaleUpsSeconds, TestingTicker ticker)
    {
        return new SinkMappingScaler(
                fullMapping,
                succinctDuration(minTimeBetweenScaleUpsSeconds, SECONDS),
                maxScaleUpGrowthFactor,
                "x",
                0,
                ticker);
    }

    private SinkMappingScaler.SinkStateProvider sinkState(
            Multimap<Integer, Long> activeBufferNodesByPartition,
            Map<Integer, Long> addedDataDistribution,
            Optional<Duration> timeSinceDataPoolLastFull)
    {
        return new SinkMappingScaler.SinkStateProvider()
        {
            @Override
            public Set<Long> getActiveBufferNodesForPartition(Integer partition)
            {
                return ImmutableSet.copyOf(activeBufferNodesByPartition.get(partition));
            }

            @Override
            public Map<Integer, Long> getAddedDataDistribution()
            {
                return addedDataDistribution;
            }

            @Override
            public Optional<Duration> timeSinceDataPoolLastFull()
            {
                return timeSinceDataPoolLastFull;
            }
        };
    }
}
