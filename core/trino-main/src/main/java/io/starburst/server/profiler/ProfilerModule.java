/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.profiler.rules.QueryProfilerRule;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ProfilerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ProfilerConfig.class);
        configBinder(binder).bindConfig(QueryProfilerConfig.class);

        binder.bind(ProfilerListener.class).in(Scopes.SINGLETON);
        binder.bind(QueryProfiler.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, QueryProfilerRule.class);
    }
}
