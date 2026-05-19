/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.starburst.server.troubleshooting.jmx.JmxTroubleshootingProvider;
import io.trino.execution.StateMachine;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static io.starburst.server.troubleshooting.TroubleshootingContext.State.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxTroubleshootingProvider
{
    @Test
    public void shouldReturnJmxAttributesWithProperTypes()
            throws IOException
    {
        ExecutorService executorService = new ForkJoinPool();
        TroubleshootingContext ctx = new TroubleshootingContext(new QueryId("1"), new StateMachine<>("stateMachine", executorService, STARTED));

        JmxTroubleshootingProvider provider = getProvider();

        provider.onContextStarted(ctx);
        provider.onContextFinished(ctx);
        Map<String, InputStream> map = provider.getInputStreams(ctx);
        assertInputStream(map.get("jmx/metrics-before.json"));
        assertInputStream(map.get("jmx/metrics-after.json"));
    }

    private void assertInputStream(InputStream is)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> parsed = mapper.readValue(is, new TypeReference<>() {});
        assertThat(parsed)
                .hasEntrySatisfying("java.util.logging:type=Logging", value -> assertThat(value)
                        .containsEntry("ObjectName", "java.util.logging:type=Logging"))
                .hasEntrySatisfying("java.lang:type=Runtime", value -> assertThat(value)
                        .hasEntrySatisfying("Uptime", uptime -> assertThat(uptime)
                                .isInstanceOf(Integer.class))
                        .hasEntrySatisfying("BootClassPathSupported", bootClassPathSupported -> assertThat(bootClassPathSupported)
                                .isInstanceOf(Boolean.class)))
                .hasEntrySatisfying("java.lang:type=OperatingSystem", value -> assertThat(value)
                        .hasEntrySatisfying("SystemLoadAverage", systemLoadAverage -> assertThat(systemLoadAverage)
                                .isInstanceOf(Double.class)));
    }

    private static JmxTroubleshootingProvider getProvider()
    {
        return new Bootstrap(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                install(new JsonModule());
                bind(JmxTroubleshootingProvider.class);
            }
        })
                .initialize()
                .getInstance(JmxTroubleshootingProvider.class);
    }
}
