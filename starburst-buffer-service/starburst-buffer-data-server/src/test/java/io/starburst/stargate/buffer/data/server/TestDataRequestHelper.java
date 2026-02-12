/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDataRequestHelper
{
    @Test
    public void testGetAsyncTimeoutWithNullClientMaxWait()
    {
        Duration result = DataRequestHelper.getAsyncTimeout(null);
        assertThat(result).isEqualTo(succinctDuration(60, SECONDS));
    }

    @Test
    public void testGetAsyncTimeoutWithZeroClientMaxWait()
    {
        Duration result = DataRequestHelper.getAsyncTimeout(succinctDuration(0, MILLISECONDS));
        assertThat(result).isEqualTo(succinctDuration(60, SECONDS));
    }

    @Test
    public void testGetAsyncTimeoutExceedsLimit()
    {
        Duration result = DataRequestHelper.getAsyncTimeout(succinctDuration(120, SECONDS));
        assertThat(result).isEqualTo(succinctDuration(60, SECONDS));
    }

    @Test
    public void testGetAsyncTimeoutWithinLimit()
    {
        Duration clientMaxWait = succinctDuration(40, SECONDS);
        Duration result = DataRequestHelper.getAsyncTimeout(clientMaxWait);

        // Should be 95% of 40 seconds = 38 seconds
        long expectedMillis = (long) (40000 * 0.95);
        assertThat(result.toMillis()).isEqualTo(expectedMillis);
    }

    @Test
    public void testGetAsyncTimeoutSmallDuration()
    {
        Duration clientMaxWait = succinctDuration(1, SECONDS);
        Duration result = DataRequestHelper.getAsyncTimeout(clientMaxWait);

        // Should be 95% of 1 second = 950ms
        assertThat(result.toMillis()).isEqualTo(950);
    }
}
