/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LicenceCheckingConnectorFactory
        implements ConnectorFactory
{
    private final Optional<StarburstFeature> feature;
    private final ConnectorFactory connectorFactory;
    private final LicenseManager licenseManager;

    public LicenceCheckingConnectorFactory(ConnectorFactory connectorFactory)
    {
        this(Optional.empty(), connectorFactory, new LicenseManagerProvider().get());
    }

    public LicenceCheckingConnectorFactory(StarburstFeature feature, ConnectorFactory connectorFactory)
    {
        this(Optional.of(feature), connectorFactory, new LicenseManagerProvider().get());
    }

    public LicenceCheckingConnectorFactory(ConnectorFactory connectorFactory, LicenseManager licenseManager)
    {
        this(Optional.empty(), connectorFactory, licenseManager);
    }

    public LicenceCheckingConnectorFactory(StarburstFeature feature, ConnectorFactory connectorFactory, LicenseManager licenseManager)
    {
        this(Optional.of(feature), connectorFactory, licenseManager);
    }

    private LicenceCheckingConnectorFactory(Optional<StarburstFeature> feature, ConnectorFactory connectorFactory, LicenseManager licenseManager)
    {
        this.feature = requireNonNull(feature, "feature is null");
        this.connectorFactory = requireNonNull(connectorFactory, "connectorFactory is null");
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    public String getName()
    {
        return connectorFactory.getName();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        feature.ifPresentOrElse(licenseManager::checkFeature, licenseManager::checkLicense);
        return connectorFactory.create(catalogName, config, context);
    }
}
