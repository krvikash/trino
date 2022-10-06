/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ChunkManagerConfig
{
    @VisibleForTesting
    static final Duration DEFAULT_EXCHANGE_STALENESS_THRESHOLD = succinctDuration(5, MINUTES);

    private DataSize chunkMaxSize = DataSize.of(16, MEGABYTE);
    private Duration exchangeStalenessThreshold = DEFAULT_EXCHANGE_STALENESS_THRESHOLD;

    @NotNull
    @MinDataSize("16MB")
    // TODO: This may not be an issue any more when we support dynamic chunk sizing.
    public DataSize getChunkMaxSize()
    {
        return chunkMaxSize;
    }

    @Config("chunk.max-size")
    @ConfigDescription("Max size of data that a chunk may accommodate")
    public ChunkManagerConfig setChunkMaxSize(DataSize chunkMaxSize)
    {
        this.chunkMaxSize = chunkMaxSize;
        return this;
    }

    @NotNull
    public Duration getExchangeStalenessThreshold()
    {
        return exchangeStalenessThreshold;
    }

    @Config("exchange.staleness-threshold")
    public ChunkManagerConfig setExchangeStalenessThreshold(Duration exchangeStalenessThreshold)
    {
        this.exchangeStalenessThreshold = exchangeStalenessThreshold;
        return this;
    }
}
