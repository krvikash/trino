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

import java.io.IOException;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;

public final class SpoolingUtils
{
    private static final char[] HEX_PREFIX_ALPHABET = "abcdef0123456789".toCharArray();

    private SpoolingUtils() {}

    public static String getFileName(long bufferNodeId, String exchangeId, long chunkId)
    {
        return getPrefixedDirectoryName(bufferNodeId, exchangeId, chunkId) + PATH_SEPARATOR + chunkId;
    }

    public static String getPrefixedDirectoryName(long bufferNodeId, String exchangeId, long chunkId)
    {
        char ch1 = HEX_PREFIX_ALPHABET[Math.abs(exchangeId.hashCode() % HEX_PREFIX_ALPHABET.length)];
        char ch2 = HEX_PREFIX_ALPHABET[(int) (chunkId % HEX_PREFIX_ALPHABET.length)];
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
}
