/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PinningPartitionNodeMapperFactory
        implements PartitionNodeMapperFactory
{
    private final BufferNodeDiscoveryManager discoveryManager;

    @Inject
    public PinningPartitionNodeMapperFactory(BufferNodeDiscoveryManager discoveryManager)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
    }

    @Override
    public PartitionNodeMapper getPartitionNodeMapper(int outputPartitionCount)
    {
        return new PinningPartitionNodeMapper(discoveryManager, outputPartitionCount);
    }
}
