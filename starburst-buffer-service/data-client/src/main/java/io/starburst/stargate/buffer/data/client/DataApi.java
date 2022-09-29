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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.OptionalLong;

public interface DataApi
{
    /**
     * List chunks for given exchange and acknowledges receiving of
     * previous set of chunks for which ChunkList.nextPagingId was set to pagingId passed in the
     * argument.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId);

    /**
     * Register existence of an exchange.
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> registerExchange(String exchangeId);

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
     * Adds data page for given (exchange, partition)
     *
     * It is expected that single writer of this method as denoted by (taskId, attemptId) pair is single threaded
     * and only sends new data page upon receiving proper response that previous page was consumed by buffer node.
     *
     * If response is not received it is responsibility of the writer to retry a request passing same dataPageId.
     * The dataPageId will be used by buffer node for deduplication purpose in case when previous request was actually registered on
     * the receiver side.
     *
     * The dataPageId is expected to be strictly increasing for subsequent data pages sent by single writer.
     *
     * @param exchangeId exchange id
     * @param partitionId partition id
     * @param taskId originating task id
     * @param attemptId originating task attempt id
     * @param dataPagesId client provided it for data pages batch for deduplication purposes
     * @param dataPages data to be recorded in an open chunk for given exchange/partition
     *
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<Void> addDataPages(String exchangeId, int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> dataPages);

    /**
     * Finish a task for given (exchange, partition)
     *
     * @param exchangeId exchange id
     * @param partitionId partition id
     * @param taskId originating task id
     * @param attemptId originating task attempt id
     */
    ListenableFuture<Void> finishTask(String exchangeId, int partitionId, int taskId, int attemptId);

    // a multi-page version of addDataPage. Not obvious if really needed. Will be added if initial tests show we need that.
    //void addDataPages(String exchangeId, int partitionId, int taskId, int attemptId, List<Long> dataPageIds, Long<Slice> dataPages);

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
     * In case of failure returned future will wrap {@link DataApiException}
     */
    ListenableFuture<List<DataPage>> getChunkData(String exchangeId, ChunkHandle chunkHandle);
}
