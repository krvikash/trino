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

import com.google.inject.Binder;
import com.google.inject.Module;

public class LicenseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(LicenseManager.class).toProvider(new LicenseManagerProvider()::get);
    }
}
