/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.status;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class StatusModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newSetBinder(binder, StatusProvider.class);
        binder.bind(StatusManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(StatusResource.class);
    }
}
