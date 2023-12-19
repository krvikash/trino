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

import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.BufferNodeInfo;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public interface DataApi
{
    /**
     * Returns node runtime information like
     * nodeId, node uri and exchange + memory statistics.
     */
    BufferNodeInfo getInfo();

    /**
     * List chunks for given exchange and acknowledges receiving of
     * previous set of chunks for which ChunkList.nextPagingId was set to pagingId passed in the
     * argument.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId);

    /**
     * Mark last set of chunks returned from {@link #listClosedChunks(String, OptionalLong)}
     * has been received. After Trino acknowledges that all chunk lists have been received,
     * we can transition the data node state to be DRAINED, and safely shut it down.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId);

    /**
     * Select chunk delivery mode for given an exchange.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> setChunkDeliveryMode(String exchangeId, ChunkDeliveryMode chunkDeliveryMode);

    /**
     * Register existence of an exchange specifying chunk delivery mode.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> registerExchange(String exchangeId, ChunkDeliveryMode chunkDeliveryMode);

    /**
     * Notification that exchange is still valid and there exists Trino coordinator which is interested in it.
     * If no exchange related request is received by BufferNode for prolonged period of time exchange data is removed from memory as if
     * {@link #removeExchange(String)} was called.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> pingExchange(String exchangeId);

    /**
     * Closes exchange and removes all in-memory data for an exchange.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> removeExchange(String exchangeId);

    /**
     * Adds data pages for given (exchange). Single request can add pages to a number of partitions.
     *
     * It is expected that single writer of this method as denoted by (taskId, attemptId) pair is single threaded
     * and only sends new data page upon receiving proper response that previous page was consumed by buffer node.
     * Concurrent requests are allowed if set of partitions written to does not overlap.
     *
     * If response is not received it is responsibility of the writer to retry a request passing same dataPagesId.
     * The dataPagesId will be used by buffer node for deduplication purpose in case when previous request was actually registered on
     * the receiver side.
     *
     * The dataPagesId is expected to be strictly increasing for subsequent data pages sent by single writer.
     *
     * @param exchangeId exchange id
     * @param taskId originating task id
     * @param attemptId originating task attempt id
     * @param dataPagesId client provided it for data pages batch for deduplication purposes
     * @param dataPagesByPartition data to be recorded in an open chunk for given exchange; pages are grouped by partitionId
     * @return server-side rate limit (if applicable)
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Optional<RateLimitInfo>> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition);

    /**
     * Mark exchange as finished. It means that no more data will be recorded for given exchange.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> finishExchange(String exchangeId);

    /**
     * Retrieves closed chunk data as list of data pages. Each data page recorded on call to addDataPage will be
     * returned as separate entry in the list.
     *
     * @param bufferNodeId id of buffer node which created chunk (from {@link ChunkHandle})
     * @param exchangeId exchange id
     * @param partitionId partition id (from {@link ChunkHandle})
     * @param chunkId chunk id (from {@link ChunkHandle})
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId);
}
