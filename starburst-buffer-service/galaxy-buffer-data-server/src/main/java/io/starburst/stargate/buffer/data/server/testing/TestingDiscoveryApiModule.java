/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server.testing;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

public class TestingDiscoveryApiModule
        extends AbstractModule
{
    @Override
    protected void configure()
    {
        binder().bind(DiscoveryApi.class).to(TestingDiscoveryApi.class).in(Scopes.SINGLETON);
    }
}
