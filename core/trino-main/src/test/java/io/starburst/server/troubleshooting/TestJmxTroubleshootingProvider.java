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
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.starburst.server.troubleshooting.jmx.JmxTroubleshootingProvider;
import io.trino.spi.QueryId;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxTroubleshootingProvider
{
    private final Injector injector = Guice.createInjector(new AbstractModule()
    {
        @Override
        protected void configure()
        {
            bind(JmxTroubleshootingProvider.class);
        }
    });

    @Inject
    private JmxTroubleshootingProvider jmxTroubleshootingProvider;

    @BeforeClass
    public void setup()
    {
        injector.injectMembers(this);
    }

    @Test
    public void shouldReturnJmxAttributesWithProperTypes()
            throws IOException
    {
        ExecutorService executorService = new ForkJoinPool();
        TroubleshootingContext ctx = new TroubleshootingContext(new QueryId("1"), executorService);

        jmxTroubleshootingProvider.onContextStarted(ctx);
        jmxTroubleshootingProvider.onContextFinished(ctx);
        Map<String, InputStream> map = jmxTroubleshootingProvider.getInputStreams(ctx);

        assertInputStream(map.get("jmx-before.json"));
        assertInputStream(map.get("jmx-after.json"));
    }

    private void assertInputStream(InputStream is)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> parsed = mapper.readValue(is, new TypeReference<>() {});
        assertThat(parsed.get("java.util.logging:type=Logging").get("ObjectName")).isEqualTo("java.util.logging:type=Logging");
        assertThat(parsed.get("java.lang:name=G1 Old Generation,type=GarbageCollector").get("LastGcInfo")).isNull();
        assertThat(parsed.get("java.lang:type=Runtime").get("Uptime")).isInstanceOf(Integer.class);
        assertThat(parsed.get("java.lang:type=Runtime").get("BootClassPathSupported")).isInstanceOf(Boolean.class);
        assertThat(parsed.get("java.lang:type=OperatingSystem").get("SystemLoadAverage")).isInstanceOf(Double.class);
    }
}
