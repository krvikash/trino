/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBufferNodeExchangeMetrics
{
    private static final JsonCodec<BufferNodeExchangeMetrics> CODEC = jsonCodec(BufferNodeExchangeMetrics.class);

    @Test
    public void testRoundTrip()
    {
        BufferNodeExchangeMetrics metrics = new BufferNodeExchangeMetrics(2, 3, 30, 4, 40, 7, 70, 15, 25);
        assertThat(CODEC.fromJson(CODEC.toJson(metrics))).isEqualTo(metrics);
    }

    @Test
    public void testResponseFromOlderBufferNode()
    {
        // buffer nodes predating the local disk metrics send only the original fields
        OriginalBufferNodeExchangeMetrics metrics = new OriginalBufferNodeExchangeMetrics(2, 3, 30, 4, 40, 7, 70);
        String json = jsonCodec(OriginalBufferNodeExchangeMetrics.class).toJson(metrics);
        assertThat(CODEC.fromJson(json)).isEqualTo(new BufferNodeExchangeMetrics(2, 3, 30, 4, 40, 7, 70, 0, 0));
    }

    @Test
    public void testResponseToOlderCoordinator()
    {
        // coordinators predating the local disk metrics decode only the original fields
        BufferNodeExchangeMetrics metrics = new BufferNodeExchangeMetrics(2, 3, 30, 4, 40, 7, 70, 15, 25);
        assertThat(jsonCodec(OriginalBufferNodeExchangeMetrics.class).fromJson(CODEC.toJson(metrics)))
                .isEqualTo(new OriginalBufferNodeExchangeMetrics(2, 3, 30, 4, 40, 7, 70));
    }

    public record OriginalBufferNodeExchangeMetrics(
            int partitionCount,
            int chunksInMemory,
            long bytesInMemory,
            int chunksSpooled,
            long bytesSpooled,
            int totalChunks,
            long totalBytes) {}
}
