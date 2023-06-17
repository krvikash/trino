/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// guard us from log flooding when log volume is too high
@ThreadSafe
public class RateLimitingLogger
{
    private static final int LOG_RATE_THRESHOLD = 5;
    private static final long RATE_WINDOW_IN_MILLIS = 5000;
    private static final Duration RATE_LIMITING_PERIOD = succinctDuration(30, TimeUnit.SECONDS);

    private final Logger logger;

    @GuardedBy("this")
    private long rateCalculationStart;
    @GuardedBy("this")
    private int rateCalculationCounter;
    @GuardedBy("this")
    private boolean suppress;
    @GuardedBy("this")
    private long suppressStart;
    @GuardedBy("this")
    private int suppressedLogs;

    public RateLimitingLogger(Logger logger)
    {
        this.logger = requireNonNull(logger, "logger is null");
        this.rateCalculationStart = System.currentTimeMillis();
    }

    public synchronized void warn(Throwable throwable, String message)
    {
        long current = System.currentTimeMillis();
        if (suppress) {
            long suppressedTimeInMillis = current - suppressStart;
            if (suppressedTimeInMillis >= RATE_LIMITING_PERIOD.toMillis()) {
                logger.info("Suppressed %d logs in the past %s period", suppressedLogs, succinctDuration(suppressedTimeInMillis, MILLISECONDS));
                suppress = false;
                rateCalculationStart = current;
                rateCalculationCounter = 1;
            }
        }
        else {
            rateCalculationCounter++;
            if (rateCalculationCounter >= LOG_RATE_THRESHOLD) {
                if (current - rateCalculationStart <= RATE_WINDOW_IN_MILLIS) {
                    // we are logging really fast, enter suppressed state
                    logger.info("Begin suppressing logs for the next %s", RATE_LIMITING_PERIOD);
                    suppress = true;
                    suppressStart = current;
                    suppressedLogs = 1;
                }
                else {
                    // log rate is fine, reset rate calculation window
                    rateCalculationStart = current;
                    rateCalculationCounter = 1;
                }
            }
        }

        if (suppress) {
            suppressedLogs++;
        }
        else {
            logger.warn(throwable, message);
        }
    }
}
