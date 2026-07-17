/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static com.starburstdata.trino.plugin.snowflake.parallel.Chunk.newInlineChunk;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestChunkFetcher
{
    // Inline chunks decode themselves from base64 and never touch the stream provider, so a real
    // (but unused) provider is sufficient to exercise ChunkFetcher without any network access.
    private static final StarburstResultStreamProvider UNUSED_STREAM_PROVIDER =
            new StarburstResultStreamProvider(HttpClients.createDefault(), new SnowflakeConfig());

    @Test
    void testRetainedSizeTracksInFlightChunk()
    {
        Chunk first = inlineChunk("first-chunk-payload");
        Chunk second = inlineChunk("second-chunk-payload-longer");
        try (ChunkFetcher fetcher = new ChunkFetcher(UNUSED_STREAM_PROVIDER, ImmutableList.of(first, second))) {
            // Nothing has been fetched yet, so no chunk buffer is held on the heap.
            assertThat(fetcher.getRetainedSizeInBytes()).isZero();

            // fetchNextChunk() sets the in-flight size synchronously, before the async fetch completes.
            fetcher.fetchNextChunk().join();
            assertThat(fetcher.getRetainedSizeInBytes()).isEqualTo(first.uncompressedByteSize());

            // Advancing to the next chunk (only happens once the previous fetch is done) updates the
            // accounted size to the newly in-flight chunk.
            fetcher.fetchNextChunk();
            assertThat(fetcher.getRetainedSizeInBytes()).isEqualTo(second.uncompressedByteSize());
        }

        // The two chunks have distinct payload sizes, so the accounting is not coincidentally equal.
        assertThat(first.uncompressedByteSize()).isNotEqualTo(second.uncompressedByteSize());
    }

    @Test
    void testFetchReturnsChunkBytesThenNullWhenExhausted()
    {
        byte[] payload = "chunk-bytes".getBytes(UTF_8);
        Chunk only = newInlineChunk(Base64.getEncoder().encodeToString(payload));
        try (ChunkFetcher fetcher = new ChunkFetcher(UNUSED_STREAM_PROVIDER, ImmutableList.of(only))) {
            assertThat(fetcher.fetchNextChunk().join()).isEqualTo(payload);
            // The single chunk is consumed; there is nothing left to fetch.
            assertThat(fetcher.fetchNextChunk()).isNull();
        }
    }

    @Test
    void testCloseReleasesAccountedBytes()
    {
        ChunkFetcher fetcher = new ChunkFetcher(UNUSED_STREAM_PROVIDER, ImmutableList.of(inlineChunk("payload")));
        fetcher.fetchNextChunk();
        assertThat(fetcher.getRetainedSizeInBytes()).isPositive();

        fetcher.close();
        assertThat(fetcher.getRetainedSizeInBytes()).isZero();
    }

    private static Chunk inlineChunk(String payload)
    {
        return newInlineChunk(Base64.getEncoder().encodeToString(payload.getBytes(UTF_8)));
    }
}
