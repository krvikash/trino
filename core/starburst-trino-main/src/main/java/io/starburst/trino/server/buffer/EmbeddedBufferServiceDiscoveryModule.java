/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.trino.server.buffer;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.starburst.stargate.buffer.discovery.server.DiscoveryServerApplicationModules.getDiscoveryServerApplicationModule;
import static io.trino.server.buffer.EmbeddedBufferServiceConfig.EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX;

public class EmbeddedBufferServiceDiscoveryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(getDiscoveryServerApplicationModule(EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX));
    }
}
