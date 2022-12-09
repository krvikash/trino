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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DiscoveryApiModule
        extends AbstractModule
{
    protected void configure()
    {
        configBinder(binder()).bindConfig(DiscoveryApiConfig.class);
    }

    @Inject
    @Provides
    public DiscoveryApi getDiscoveryApi(DiscoveryApiConfig config, @ForBufferDiscoveryClient HttpClient httpClient)
    {
        return new HttpDiscoveryClient(config.getDiscoveryServiceUri(), httpClient);
    }
}
