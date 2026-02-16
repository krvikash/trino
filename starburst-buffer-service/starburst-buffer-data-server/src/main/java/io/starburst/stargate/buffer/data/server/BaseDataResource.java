/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.client.ErrorCode.USER_ERROR;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.RATE_LIMIT_HEADER;
import static io.starburst.stargate.buffer.data.server.HttpResponseHelper.errorResponse;
import static java.util.Objects.requireNonNull;

public abstract class BaseDataResource
{
    private static final Logger logger = Logger.get(BaseDataResource.class);

    protected final long bufferNodeId;
    protected final ChunkManager chunkManager;
    protected final MemoryAllocator memoryAllocator;
    protected final BufferNodeStateManager bufferNodeStateManager;
    protected final BufferNodeInfoService bufferNodeInfoService;
    protected final JsonCodec<Span> spanJsonCodec;
    protected final boolean dataIntegrityVerificationEnabled;
    protected final boolean dropUploadedPages;
    protected final DataServerStats stats;
    protected final CounterStat writtenDataSize;
    protected final DistributionStat writtenDataSizeDistribution;
    protected final DistributionStat writtenDataSizePerPartitionDistribution;
    protected final CounterStat readDataSize;
    protected final DistributionStat readDataSizeDistribution;
    protected final AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator;
    protected final AddDataPagesInProgressTracker inProgressTracker;
    protected final int maxInProgressAddDataPagesRequests;

    protected BaseDataResource(
            BufferNodeId bufferNodeId,
            ChunkManager chunkManager,
            MemoryAllocator memoryAllocator,
            BufferNodeStateManager bufferNodeStateManager,
            DataServerConfig config,
            BufferNodeInfoService bufferNodeInfoService,
            AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator,
            AddDataPagesInProgressTracker inProgressTracker,
            JsonCodec<Span> spanJsonCodec,
            DataServerStats stats)
    {
        this.bufferNodeId = requireNonNull(bufferNodeId, "bufferNodeId is null").getLongValue();
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
        this.bufferNodeStateManager = requireNonNull(bufferNodeStateManager, "bufferNodeStateManager is null");
        this.dataIntegrityVerificationEnabled = config.isDataIntegrityVerificationEnabled();
        this.dropUploadedPages = config.isTestingDropUploadedPages();
        this.bufferNodeInfoService = requireNonNull(bufferNodeInfoService, "bufferNodeInfoService is null");
        this.addDataPagesThrottlingCalculator = requireNonNull(addDataPagesThrottlingCalculator, "addDataPagesThrottlingCalculator is null");
        this.inProgressTracker = requireNonNull(inProgressTracker, "inProgressTracker is null");
        this.spanJsonCodec = requireNonNull(spanJsonCodec, "spanJsonCodec is null");
        this.maxInProgressAddDataPagesRequests = config.getMaxInProgressAddDataPagesRequests();

        this.stats = requireNonNull(stats, "stats is null");
        this.writtenDataSize = stats.getWrittenDataSize();
        this.writtenDataSizeDistribution = stats.getWrittenDataSizeDistribution();
        this.writtenDataSizePerPartitionDistribution = stats.getWrittenDataSizePerPartitionDistribution();
        this.readDataSize = stats.getReadDataSize();
        this.readDataSizeDistribution = stats.getReadDataSizeDistribution();
    }

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getInfo(@QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            return Response.ok(bufferNodeInfoService.getNodeInfo()).build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /info");
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/markAllClosedChunksReceived")
    @Produces(MediaType.TEXT_PLAIN)
    public Response markAllClosedChunksReceived(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.markAllClosedChunksReceived(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /%s/markAllClosedChunksReceived", exchangeId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/setChunkDeliveryMode")
    @Produces(MediaType.TEXT_PLAIN)
    public Response setChunkDeliveryMode(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("chunkDeliveryMode") ChunkDeliveryMode chunkDeliveryMode,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.setChunkDeliveryMode(exchangeId, chunkDeliveryMode);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /%s/setChunkDeliveryMode?chunkDeliveryMode=%s", exchangeId, chunkDeliveryMode);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/register")
    @Produces(MediaType.TEXT_PLAIN)
    public Response registerExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("chunkDeliveryMode") @Nullable ChunkDeliveryMode chunkDeliveryMode,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
            @QueryParam("exchangeSpan") @Nullable String serializedExchangeSpan)
    {
        try {
            // registerExchange call can happen after node already started draining.
            // There is a temptation to just reject request here with DRAINING error code, but it would not be correct.
            // When Trino coordinator calls registerExchange it could be that data was already written by some worker to the exchange
            // and Trino coordinator must go through registration to start polling for data chunks.
            //
            // Consider following flow of events:
            // * Trino worker call addDataPages (it implicitly register exchange in data node)
            // * Data node start draining
            // * Trino coordinator calls registerExchange
            // at this point Trino coordinator must proceed with polling for chunks which would not happen if `registerExchange` returned DRAINING error code.
            checkTargetBufferNodeId(targetBufferNodeId);
            ChunkDeliveryMode mode = Optional.ofNullable(chunkDeliveryMode).orElse(STANDARD);
            Optional<Span> exchangeSpan = Optional.ofNullable(serializedExchangeSpan).map(spanJsonCodec::fromJson);
            chunkManager.registerExchange(exchangeId, mode, exchangeSpan);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /%s/register", exchangeId);
            return errorResponse(e);
        }
    }

    @GET
    @Path("{exchangeId}/ping")
    @Produces(MediaType.APPLICATION_JSON)
    public Response pingExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            return Response.ok(chunkManager.pingExchange(exchangeId)).build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on GET /%s/ping", exchangeId);
            return errorResponse(e);
        }
    }

    @DELETE
    @Path("{exchangeId}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response removeExchange(
            @PathParam("exchangeId") String exchangeId,
            @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId)
    {
        try {
            checkTargetBufferNodeId(targetBufferNodeId);
            chunkManager.removeExchange(exchangeId);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            reportException(logger, e, "error on DELETE /%s", exchangeId);
            return errorResponse(e);
        }
    }

    protected void checkTargetBufferNodeId(@Nullable Long targetBufferNodeId)
    {
        if (targetBufferNodeId == null) {
            return;
        }
        if (bufferNodeId != targetBufferNodeId) {
            throw new DataServerException(USER_ERROR, "target buffer node mismatch (%s vs %s)".formatted(targetBufferNodeId, bufferNodeId));
        }
    }

    @FormatMethod
    protected static void reportException(Logger logger, Throwable e, @FormatString String format, Object... args)
    {
        if (e instanceof DataApiException) {
            logger.debug(e, format, args);
        }
        else {
            logger.warn(e, format, args);
        }
    }

    protected void recordAddDataPagesRequest(long start, String clientId)
    {
        addDataPagesThrottlingCalculator.recordProcessTimeInMillis(System.currentTimeMillis() - start);
        addDataPagesThrottlingCalculator.updateCounterStat(clientId, 1);
    }

    protected Map<String, String> getRateLimitHeaders(String clientId)
    {
        OptionalDouble rateLimit = addDataPagesThrottlingCalculator.getRateLimit(clientId, inProgressTracker.getInProgressAddDataPagesRequests());
        if (rateLimit.isPresent()) {
            return ImmutableMap.of(
                    RATE_LIMIT_HEADER, Double.toString(rateLimit.getAsDouble()),
                    AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER, Long.toString(addDataPagesThrottlingCalculator.getAverageProcessTimeInMillis()));
        }
        return ImmutableMap.of();
    }
}
