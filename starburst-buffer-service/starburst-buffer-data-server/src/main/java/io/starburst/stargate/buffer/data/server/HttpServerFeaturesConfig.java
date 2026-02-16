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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

/**
 * Configuration for HTTP Server features.
 */
public class HttpServerFeaturesConfig
{
    public static final String VIRTUAL_THREADS_CONFIG_PREFIX = "virtual-threads";

    private boolean virtualThreadsEnabled;

    public boolean isVirtualThreadsEnabled()
    {
        return virtualThreadsEnabled;
    }

    @Config("virtual-threads.enabled")
    @ConfigDescription("Enable Virtual Threads for HTTP server request handling")
    public HttpServerFeaturesConfig setVirtualThreadsEnabled(boolean virtualThreadsEnabled)
    {
        this.virtualThreadsEnabled = virtualThreadsEnabled;
        return this;
    }
}
