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

import com.google.common.io.ByteSource;

import java.util.Optional;

interface LicenseProvider
{
    Optional<License> getLicense();

    default Optional<ByteSource> getFileHandle()
    {
        return Optional.empty();
    }
}
