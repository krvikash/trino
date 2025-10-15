/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.starburstdata.trino.plugin.stargate.StargateConfig;
import com.starburstdata.trino.plugin.stargate.StargateSslConfig;
import io.trino.client.OkHttpSegmentLoader;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.uri.HttpClientFactory;
import io.trino.client.uri.TrinoUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import static com.starburstdata.trino.plugin.stargate.TrinoUriFactory.sslConnectionProperties;

public class SegmentLoaderProvider
        implements Provider<SegmentLoader>
{
    private final TrinoUri trinoUri;

    @Inject
    public SegmentLoaderProvider(StargateConfig connectorConfig, StargateSslConfig stargateSslConfig, BaseJdbcConfig config)
    {
        this.trinoUri = TrinoUri.create(
                config.getConnectionUrl().replaceFirst("jdbc:", ""),
                sslConnectionProperties(connectorConfig, stargateSslConfig));
    }

    @Override
    public SegmentLoader get()
    {
        return new LazySegmentLoader(() -> new OkHttpSegmentLoader(HttpClientFactory
                .unauthenticatedClientBuilder(trinoUri, "stargate-parallel")
                .build()));
    }
}
