/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.providers;

import com.google.common.collect.ImmutableMap;
import io.starburst.server.troubleshooting.TroubleshootingContext;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface TroubleshootingProvider
{
    default Map<String, InputStream> getInputStreams(TroubleshootingContext context)
    {
        return ImmutableMap.of();
    }

    static InputStream toInputStream(String value)
    {
        return new ByteArrayInputStream(value.getBytes(UTF_8));
    }

    default void onContextStarted(TroubleshootingContext context) {}

    default void onContextFinished(TroubleshootingContext context) {}

    default void onContextRemoved(TroubleshootingContext context) {}
}
