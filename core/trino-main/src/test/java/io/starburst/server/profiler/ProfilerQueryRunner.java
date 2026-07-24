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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.StandaloneQueryRunner;

import static io.trino.SessionTestUtils.TEST_SESSION;

public final class ProfilerQueryRunner
{
    private ProfilerQueryRunner() {}

    public static StandaloneQueryRunner create(ProfilerResultSink sink)
    {
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                        {
                            @Override
                            protected void setup(Binder binder)
                            {
                                buildConfigObject(ProfilerConfig.class);
                                install(new ProfilerModule());
                                binder.bind(ProfilerResultSink.class).toInstance(sink);
                            }
                        })
                        .setProperties(ImmutableMap.<String, String>builder()
                                .put("profiler.enabled", "true")
                                .put("profiler.min-query-duration", "0s")
                                .put("profiler.thread-pool-size", "1")
                                .put("profiler.max-queue-size", "1")
                                .buildOrThrow()));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().orElseThrow(), "tpch", ImmutableMap.of());
        return queryRunner;
    }
}
