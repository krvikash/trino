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

import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Objects.requireNonNull;

public class LicenseCheckingPasswordAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    private final PasswordAuthenticatorFactory delegate;
    private final Supplier<LicenseManager> licenseManager;

    public LicenseCheckingPasswordAuthenticatorFactory(PasswordAuthenticatorFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "passwordAuthenticatorFactory is null");
        this.licenseManager = memoize(new LicenseManagerProvider()::get);
    }

    public PasswordAuthenticatorFactory getDelegate()
    {
        return delegate;
    }

    @Override
    public String getName()
    {
        return delegate.getName();
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        licenseManager.get().checkLicense();
        return delegate.create(config);
    }
}
