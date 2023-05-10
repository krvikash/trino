/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import static com.google.common.base.Preconditions.checkArgument;

public record RateLimitInfo(
        double rateLimit,
        long averageProcessTimeInMillis)
{
    public RateLimitInfo {
        checkArgument(rateLimit > 0, "rateLimit %s is not positive", rateLimit);
        checkArgument(averageProcessTimeInMillis > 0, "averageProcessTimeInMillis %s is not positive", averageProcessTimeInMillis);
    }
}
