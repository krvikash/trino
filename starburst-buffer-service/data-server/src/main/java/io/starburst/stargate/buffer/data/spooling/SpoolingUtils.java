/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.CHUNK_FILE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;

public final class SpoolingUtils
{
    private static final char[] HEX_PREFIX_ALPHABET = "abcdef0123456789".toCharArray();
    private static final String SPOOLING_METADATA_FILE_SUFFIX = ".spooling.metadata";

    private SpoolingUtils() {}

    public static String getFileName(long bufferNodeId, String exchangeId, long chunkId)
    {
        return getPrefixedDirectoryName(bufferNodeId, exchangeId, chunkId) + PATH_SEPARATOR + chunkId;
    }

    public static String getPrefixedDirectoryName(long bufferNodeId, String exchangeId, long salt)
    {
        // This is to ensure good load balancing between S3 shards.
        // AWS set up partitioning for us based on the first two characters.
        char ch1 = HEX_PREFIX_ALPHABET[Math.abs(exchangeId.hashCode() % HEX_PREFIX_ALPHABET.length)];
        char ch2 = HEX_PREFIX_ALPHABET[(int) (salt % HEX_PREFIX_ALPHABET.length)];
        return ch1 + "" + ch2 + "." + exchangeId + "." + bufferNodeId;
    }

    public static List<String> getPrefixedDirectories(long bufferNodeId, String exchangeId)
    {
        ImmutableList.Builder<String> prefixedDirectories = ImmutableList.builder();
        for (long chunkId = 0; chunkId < HEX_PREFIX_ALPHABET.length; ++chunkId) {
            prefixedDirectories.add(getPrefixedDirectoryName(bufferNodeId, exchangeId, chunkId));
        }
        return prefixedDirectories.build();
    }

    // Helper function that translates exceptions to avoid abstraction leak
    public static <T> ListenableFuture<T> translateFailures(ListenableFuture<T> listenableFuture)
    {
        return Futures.catchingAsync(listenableFuture, Throwable.class, throwable -> {
            if (throwable instanceof Error || throwable instanceof IOException) {
                return immediateFailedFuture(throwable);
            }
            return immediateFailedFuture(new IOException(throwable));
        }, directExecutor());
    }

    public static void writeChunkDataLease(ChunkDataLease chunkDataLease, Consumer<ByteBuffer> consumer)
    {
        SliceOutput sliceOutput = Slices.allocate(CHUNK_FILE_HEADER_SIZE).getOutput();
        sliceOutput.writeLong(chunkDataLease.checksum());
        sliceOutput.writeInt(chunkDataLease.numDataPages());
        consumer.accept(ByteBuffer.wrap(sliceOutput.slice().byteArray()));
        for (Slice chunkSlice : chunkDataLease.chunkSlices()) {
            consumer.accept(ByteBuffer.wrap(chunkSlice.byteArray(), chunkSlice.byteArrayOffset(), chunkSlice.length()));
        }
    }

    public static String getMetadataFileName(long bufferNodeId)
    {
        return bufferNodeId + SPOOLING_METADATA_FILE_SUFFIX;
    }
}
