/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.inject.Binder;
import com.google.inject.Module;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class ServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        jaxrsBinder(binder).bind(DiscoveryResource.class);
    }
}
