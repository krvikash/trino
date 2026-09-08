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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkNotFoundException;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReaderException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.http.client.HttpStatus.NOT_FOUND;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLED_CHUNK_LENGTH_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLED_CHUNK_OFFSET_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.SPOOLING_FILE_LOCATION_HEADER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHttpDataClient
{
    private static final String EXCHANGE_0 = "exchange-0";
    private static final long BUFFER_NODE_ID = 1;
    private static final int PARTITION_ID = 3;
    private static final long CHUNK_ID = 178;
    private static final SpooledChunk SPOOLED_CHUNK = new SpooledChunk("file:///tmp/spooling/exchange-0/178", 0, 42);

    @Test
    public void testMissingSpooledChunkReportedAsChunkNotFound()
    {
        // the buffer node still knows about the chunk but the spooled data behind it is gone, so no number of read retries can recover it
        HttpDataClient dataClient = createDataClient(_ -> immediateFailedFuture(new SpooledChunkNotFoundException("no such file")));

        assertThatThrownBy(() -> getFutureValue(dataClient.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, PARTITION_ID, CHUNK_ID)))
                .isInstanceOfSatisfying(DataApiException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.CHUNK_NOT_FOUND))
                .hasMessageContaining(SPOOLED_CHUNK.location())
                .hasMessageContaining("reading chunk %s/%s/%s/%s".formatted(BUFFER_NODE_ID, EXCHANGE_0, PARTITION_ID, CHUNK_ID))
                .cause()
                .isInstanceOf(SpooledChunkNotFoundException.class);
    }

    @Test
    public void testOtherSpoolingFailureNotReportedAsChunkNotFound()
    {
        // a spooling read may fail for transient reasons; those must stay distinguishable from data loss so that callers keep retrying
        HttpDataClient dataClient = createDataClient(_ -> immediateFailedFuture(new SpooledChunkReaderException("connection reset")));

        assertThatThrownBy(() -> getFutureValue(dataClient.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, PARTITION_ID, CHUNK_ID)))
                .isInstanceOf(SpooledChunkReaderException.class)
                .isNotInstanceOf(DataApiException.class);
    }

    @Test
    public void testSpooledChunkRead()
    {
        DataPage dataPage = new DataPage(0, 0, Slices.utf8Slice("page"));
        HttpDataClient dataClient = createDataClient(spooledChunk -> {
            assertThat(spooledChunk).isEqualTo(SPOOLED_CHUNK);
            return immediateFuture(ImmutableList.of(dataPage));
        });

        DataApi.ChunkDataResponse response = getFutureValue(dataClient.getChunkData(BUFFER_NODE_ID, EXCHANGE_0, PARTITION_ID, CHUNK_ID));
        assertThat(response.pages()).containsExactly(dataPage);
        assertThat(response.readFromSpoolingStorage()).isTrue();
    }

    private static HttpDataClient createDataClient(Function<SpooledChunk, ListenableFuture<List<DataPage>>> spooledChunkReader)
    {
        // mirrors how the data server hands back a spooled chunk reference instead of the chunk data itself
        TestingHttpClient httpClient = new TestingHttpClient(_ -> new TestingResponse(
                NOT_FOUND,
                ImmutableListMultimap.of(
                        SPOOLING_FILE_LOCATION_HEADER, SPOOLED_CHUNK.location(),
                        SPOOLED_CHUNK_OFFSET_HEADER, String.valueOf(SPOOLED_CHUNK.offset()),
                        SPOOLED_CHUNK_LENGTH_HEADER, String.valueOf(SPOOLED_CHUNK.length())),
                new byte[0]));

        return new HttpDataClient(
                URI.create("http://testing"),
                BUFFER_NODE_ID,
                httpClient,
                new Duration(30, SECONDS),
                new TestingSpooledChunkReader(spooledChunkReader),
                false,
                Optional.empty(),
                JsonCodec.jsonCodec(Span.class));
    }

    private static class TestingSpooledChunkReader
            implements SpooledChunkReader
    {
        private final Function<SpooledChunk, ListenableFuture<List<DataPage>>> readChunk;

        public TestingSpooledChunkReader(Function<SpooledChunk, ListenableFuture<List<DataPage>>> readChunk)
        {
            this.readChunk = readChunk;
        }

        @Override
        public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
        {
            return readChunk.apply(spooledChunk);
        }

        @Override
        public void close() {}
    }
}
