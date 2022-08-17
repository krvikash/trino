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

import java.util.List;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * @param chunks List of chunk handles for closed chunks which can be retrieved from the buffer node.
 * @param nextPagingId Paging id to be used for next request to DataApi.listChunks.
 *                     If nextPagingId is empty it means all chunks have been returned and caller should not expect any more chunks
 *                     for this exchange from this buffer node. {@link #chunks will be empty then}.
 */
public record ChunkList(
        List<ChunkHandle> chunks,
        OptionalLong nextPagingId) {
    public ChunkList
    {
        chunks = ImmutableList.copyOf(requireNonNull(chunks, "chunks is null"));
        requireNonNull(nextPagingId, "nextPagingId is null");
    }
}
