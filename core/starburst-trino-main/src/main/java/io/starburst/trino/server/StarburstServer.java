/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.starburst.stargate.buffer.data.server.BufferNodeStateManager;
import io.trino.server.Server;

import static io.starburst.stargate.buffer.BufferNodeState.STARTED;

public class StarburstServer
        extends Server
{
    @Override
    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of(new BufferServiceModule());
    }

    @Override
    protected void additionalStartup(Injector injector)
    {
        if (injector.getExistingBinding(Key.get(BufferNodeStateManager.class)) != null) {
            // mark embedded buffer service as started
            injector.getInstance(BufferNodeStateManager.class).transitionState(STARTED);
        }
    }
}
