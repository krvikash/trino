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

import com.google.common.collect.ImmutableSet;

import java.util.function.Supplier;

public class LicenseManagerProvider
        implements Supplier<LicenseManager>
{
    @Override
    public LicenseManager get()
    {
        return new StarburstLicenseManager(
                ImmutableSet.<LicenseProvider>builder()
                        .add(new JSONLicenseProvider())
                        .add(new AWSMarketplaceLicenseProvider())
                        .add(new ManagedKubernetesLicenseProvider())
                        .build());
    }
}
