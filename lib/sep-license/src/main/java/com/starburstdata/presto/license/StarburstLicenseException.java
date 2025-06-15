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

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;

class StarburstLicenseException
        extends TrinoException
{
    public StarburstLicenseException(String message)
    {
        super(CONFIGURATION_INVALID, message);
    }
}
