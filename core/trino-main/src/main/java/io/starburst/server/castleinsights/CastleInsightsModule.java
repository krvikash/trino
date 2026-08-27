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
    }
}
