/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.castleinsights.analyzer.CastleInsightAnalyzer;
import io.starburst.server.castleinsights.analyzer.CastleInsightsExtractor;
import io.starburst.server.castleinsights.analyzer.CastleInsightsStats;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CastleInsightsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CastleInsightsConfig.class);

        Multibinder.newSetBinder(binder, CastleInsightAnalyzer.class);

        binder.bind(CastleInsightsExtractor.class).in(Scopes.SINGLETON);
        binder.bind(CastleInsightsStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CastleInsightsStats.class).withGeneratedName();

        // Cork has nowhere to ship insights; an integration overrides this with its own transport.
        newOptionalBinder(binder, CastleInsightsResultSink.class)
                .setDefault().to(DiscardingCastleInsightsResultSink.class).in(Scopes.SINGLETON);

        // Nothing injects the listener, it registers itself, so it has to be created eagerly.
        binder.bind(CastleInsightsListener.class).asEagerSingleton();
    }
}
