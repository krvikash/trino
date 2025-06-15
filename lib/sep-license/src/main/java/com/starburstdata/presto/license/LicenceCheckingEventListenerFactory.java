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

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LicenceCheckingEventListenerFactory
        implements EventListenerFactory
{
    private final EventListenerFactory delegate;
    private final LicenseManager licenseManager;

    public LicenceCheckingEventListenerFactory(EventListenerFactory delegate)
    {
        this(delegate, new LicenseManagerProvider().get());
    }

    public LicenceCheckingEventListenerFactory(EventListenerFactory delegate, LicenseManager licenseManager)
    {
        this.delegate = requireNonNull(delegate, "eventListenerFactory is null");
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    public EventListenerFactory getDelegate()
    {
        return delegate;
    }

    @Override
    public String getName()
    {
        return delegate.getName();
    }

    @Override
    public EventListener create(Map<String, String> config, EventListenerContext context)
    {
        licenseManager.checkLicense();
        return delegate.create(config, context);
    }
}
