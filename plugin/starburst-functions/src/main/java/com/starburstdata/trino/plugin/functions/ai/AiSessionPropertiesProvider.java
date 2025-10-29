/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class AiSessionPropertiesProvider
        implements SessionPropertiesProvider
{
    private static final String BATCH_CALLING_ENABLED = "batch_calling_enabled";

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                booleanProperty(
                        BATCH_CALLING_ENABLED,
                        "Enable batch calling calling convention where supported",
                        false,
                        false));
    }

    public static boolean isBatchCallingEnabled(ConnectorSession session)
    {
        return session.getProperty(BATCH_CALLING_ENABLED, Boolean.class);
    }
}
