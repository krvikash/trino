/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAiClientModule
{
    @Test
    public void testNoScopeUsesAmbientTracerUnchanged()
    {
        AiClientModule module = new AiClientModule(ModelConnectionSpecsLoader.EMPTY_LOADER);
        OpenTelemetry openTelemetry = OpenTelemetry.noop();
        Tracer ambientTracer = openTelemetry.getTracer("ambient");

        // This is the regression guard for the AI SQL function path: with no scope passed
        // (IcebergConnectorFactory, LakehouseConnectorFactory, and starburst-functions' AiModule
        // all call this constructor), @ForAiClient Tracer must be the SAME instance as the
        // ambient catalog Tracer, not a different one that happens to behave the same.
        assertThat(module.createAiClientTracer(openTelemetry, ambientTracer)).isSameAs(ambientTracer);
    }

    @Test
    public void testScopeUsesNamedTracer()
    {
        AiClientModule module = new AiClientModule(ModelConnectionSpecsLoader.EMPTY_LOADER, "io.starburst.ai.agent");
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder().build();
        Tracer ambientTracer = openTelemetry.getTracer("ambient");

        Tracer aiClientTracer = module.createAiClientTracer(openTelemetry, ambientTracer);

        assertThat(aiClientTracer).isNotSameAs(ambientTracer);
    }
}
