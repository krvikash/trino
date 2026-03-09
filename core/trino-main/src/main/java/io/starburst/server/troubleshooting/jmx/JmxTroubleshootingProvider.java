/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jmx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.TriConsumer;
import io.starburst.server.troubleshooting.TroubleshootingContext;
import io.starburst.server.troubleshooting.providers.TroubleshootingProvider;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static io.starburst.server.troubleshooting.providers.TroubleshootingProvider.toInputStream;
import static java.util.Objects.requireNonNull;
import static javax.management.ObjectName.WILDCARD;

public class JmxTroubleshootingProvider
        implements TroubleshootingProvider
{
    private final MBeanServer mBeanServer;
    private final ObjectMapper objectMapper;

    @Inject
    public JmxTroubleshootingProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        JmxTroubleshootingContext ctx = context.getOrThrow(JmxTroubleshootingContext.class);
        try {
            return ImmutableMap.of(
                    "jmx/metrics-before.json", toInputStream(objectMapper.writeValueAsString(ctx.getBefore())),
                    "jmx/metrics-after.json", toInputStream(objectMapper.writeValueAsString(ctx.getAfter())));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onContextStarted(TroubleshootingContext context)
    {
        JmxTroubleshootingContext ctx = new JmxTroubleshootingContext();
        extractJmxAttributeValues(ctx::putBefore);
        context.set(JmxTroubleshootingContext.class, ctx);
    }

    @Override
    public void onContextFinished(TroubleshootingContext context)
    {
        JmxTroubleshootingContext ctx = context.getOrThrow(JmxTroubleshootingContext.class);
        extractJmxAttributeValues(ctx::putAfter);
    }

    private void extractJmxAttributeValues(TriConsumer<String, String, Object> mapper)
    {
        Set<ObjectName> objectNames = mBeanServer.queryNames(WILDCARD, null);
        for (ObjectName objectName : objectNames) {
            try {
                mBeanServer.getAttributes(objectName, Arrays.stream(mBeanServer.getMBeanInfo(objectName).getAttributes())
                                .map(MBeanAttributeInfo::getName)
                                .toArray(String[]::new))
                            .asList()
                            .forEach(att -> mapper.accept(objectName.getCanonicalName(), att.getName(), attributeToType(att.getValue())));
            }
            catch (InstanceNotFoundException e) {
                // Mbean was removed after call to the queryNames, let's ignore it.
            }
            catch (ReflectionException | IntrospectionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Object attributeToType(Object value)
    {
        if (null == value) {
            return null;
        }
        if (value instanceof Double && value.equals(Double.NaN)) {
            return null;
        }
        if (value instanceof Float && value.equals(Float.NaN)) {
            return null;
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value;
        }

        return value.toString();
    }
}
