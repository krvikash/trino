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

import static java.util.Objects.requireNonNull;

public enum AWSPartition
{
    AWS(""),
    AWS_CN("cn"),
    AWS_US_GOV("us-gov");

    private final String prefix;

    AWSPartition(String prefix)
    {
        this.prefix = requireNonNull(prefix);
    }

    public static AWSPartition fromRegion(String region)
    {
        for (AWSPartition partition : AWSPartition.values()) {
            if (partition != AWS && region.startsWith(partition.prefix)) {
                return partition;
            }
        }
        return AWS;
    }
}
