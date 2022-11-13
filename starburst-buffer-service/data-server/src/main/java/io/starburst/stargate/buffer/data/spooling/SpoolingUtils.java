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
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;

import java.io.IOException;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.UnsafeSlice.getIntUnchecked;
import static io.airlift.slice.UnsafeSlice.getLongUnchecked;

public final class SpoolingUtils
{
    public static final String PATH_SEPARATOR = "/";
    public static final int CHUNK_FILE_HEADER_SIZE = Long.BYTES + Integer.BYTES; // checksum, numDataPages
    public static final int CHECKSUM_OFFSET = 0;
    public static final int NUM_DATA_PAGES_OFFSET = Long.BYTES;
    private static final char[] HEX_PREFIX_ALPHABET = "abcdef0123456789".toCharArray();

    private SpoolingUtils() {}

    public static String getFileName(String exchangeId, long chunkId, long bufferNodeId)
    {
        return getPrefixedDirectoryName(exchangeId, chunkId) + PATH_SEPARATOR + chunkId + "-" + bufferNodeId;
    }

    public static String getPrefixedDirectoryName(String exchangeId, long chunkId)
    {
        char ch1 = HEX_PREFIX_ALPHABET[Math.abs(exchangeId.hashCode() % HEX_PREFIX_ALPHABET.length)];
        char ch2 = HEX_PREFIX_ALPHABET[(int) (chunkId % HEX_PREFIX_ALPHABET.length)];
        return ch1 + "" + ch2 + "." + exchangeId;
    }

    public static List<String> getPrefixedDirectories(String exchangeId)
    {
        ImmutableList.Builder<String> prefixedDirectories = ImmutableList.builder();
        for (long chunkId = 0; chunkId < HEX_PREFIX_ALPHABET.length; ++chunkId) {
            prefixedDirectories.add(getPrefixedDirectoryName(exchangeId, chunkId));
        }
        return prefixedDirectories.build();
    }

    public static ChunkDataHolder getChunkDataHolder(Slice slice)
    {
        long checksum = getLongUnchecked(slice, CHECKSUM_OFFSET);
        int numDataPages = getIntUnchecked(slice, NUM_DATA_PAGES_OFFSET);
        List<Slice> chunkSlices = ImmutableList.of(slice.slice(CHUNK_FILE_HEADER_SIZE, slice.length() - CHUNK_FILE_HEADER_SIZE));
        return new ChunkDataHolder(chunkSlices, checksum, numDataPages);
    }
}
