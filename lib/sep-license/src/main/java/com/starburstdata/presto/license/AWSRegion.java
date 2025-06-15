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

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public enum AWSRegion
{
    US_GOV_EAST_1("us-gov-east-1"),
    US_GOV_WEST_1("us-gov-west-1");

    private final String name;

    public String getName()
    {
        return name;
    }

    AWSRegion(String name)
    {
        this.name = requireNonNull(name);
    }

    public static AWSRegion fromName(String name)
    {
        return AWSRegion.valueOf(name.toUpperCase(ENGLISH).replace("-", "_"));
    }
}
