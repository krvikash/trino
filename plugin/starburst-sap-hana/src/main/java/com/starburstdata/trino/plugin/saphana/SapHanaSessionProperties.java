/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.NO_PARALLELISM;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public final class SapHanaSessionProperties
        implements SessionPropertiesProvider
{
    public static final String PARALLELISM_TYPE = "parallelism_type";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SapHanaSessionProperties(LicenseManager licenseManager, SapHanaConfig starburstOracleConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        PARALLELISM_TYPE,
                        "Parallelism strategy for reads",
                        SapHanaParallelismType.class,
                        starburstOracleConfig.getParallelismType(),
                        value -> {
                            if (value != NO_PARALLELISM) {
                                licenseManager.checkLicense();
                            }
                        },
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static SapHanaParallelismType getParallelismType(ConnectorSession session)
    {
        return session.getProperty(PARALLELISM_TYPE, SapHanaParallelismType.class);
    }
}
