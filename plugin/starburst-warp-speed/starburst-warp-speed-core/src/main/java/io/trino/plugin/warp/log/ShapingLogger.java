/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.log;

import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.trino.plugin.warp.tools.util.Pair;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * this Logger counts similar logs and only outputs a single log message based on threshold / intervalInMillis
 */
public class ShapingLogger
{
    private static final Map<Logger, ShapingLogger> instances = new ConcurrentHashMap<>();

    private static final String FORMAT = "%s - skipped %d times";
    private final Map<Pair<String, List<Object>>, ShapingLoggerState> shapingLoggerStateMap = new ConcurrentHashMap<>();
    private final Logger logger;

    //threshold for log flushing. if threshold<=0 then threshold is ignored
    private final int threshold;

    //interval in millis for log flushing. if duration = Duration.ZERO then it is ignored
    long durationMillis;

    //if shapeMode=FORMAT then we accumulate by format string
    //if shapeMode=FULL then we accumulate by formatted string
    private final MODE mode;

    //if true first log message will be logged regardless of interval/duration
    private final int numberOfSamples;

    private ShapingLogger(Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamples,
            MODE mode)
    {
        this.logger = requireNonNull(logger);
        this.threshold = threshold;
        durationMillis = (duration != null ? duration : Duration.ZERO).toMillis();
        this.numberOfSamples = numberOfSamples;
        checkArgument(threshold == 0 || threshold > numberOfSamples, "threshold must be greater than number of samples");
        this.mode = requireNonNull(mode);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration)
    {
        return getInstance(logger, threshold, duration, 1);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamplings)
    {
        return getInstance(logger, threshold, duration, numberOfSamplings, MODE.FORMAT);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamplings,
            MODE mode)
    {
        return instances.computeIfAbsent(logger, _ -> new ShapingLogger(logger, threshold, duration, numberOfSamplings, mode));
    }

    public void info(String message)
    {
        info("%s", message);
    }

    @FormatMethod
    public void info(final String format, Object... args)
    {
        if (logger.isInfoEnabled()) {
            log(getKey(format, args), () -> logger.info(format, args));
        }
    }

    public void warn(String message)
    {
        warn("%s", message);
    }

    @FormatMethod
    public void warn(final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.warn(format, args));
    }

    @FormatMethod
    public void warn(Throwable exception, final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.warn(exception, format, args));
    }

    public void error(String message)
    {
        error("%s", message);
    }

    @FormatMethod
    public void error(final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.error(format, args));
    }

    @FormatMethod
    public void error(Throwable e, final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.error(e, format, args));
    }

    private void log(Pair<String, List<Object>> key, Runnable runnable)
    {
        long currentTimeMillis = System.currentTimeMillis();
        shapingLoggerStateMap.compute(key, (_, val) -> {
            if (val == null) {
                val = new ShapingLoggerState(1, currentTimeMillis);
            }

            long lastLogTime = val.lastLogTime();
            int count = val.count();
            if (count <= numberOfSamples) {
                runnable.run();
            }

            if ((threshold > 0 && (count == threshold)) ||
                    ((durationMillis > 0) && currentTimeMillis - lastLogTime > durationMillis)) {
                if (count > numberOfSamples) {
                    logger.info(getOccurrencesMessage(key, count - numberOfSamples));
                }
                lastLogTime = currentTimeMillis;
                count = 0;
            }
            return new ShapingLoggerState(count + 1, lastLogTime);
        });
    }

    private Pair<String, List<Object>> getKey(String message, Object... args)
    {
        if (mode.equals(MODE.FORMAT)) {
            return Pair.of(message, List.of());
        }
        return Pair.of(message, Arrays.asList(args));
    }

    private String getOccurrencesMessage(Pair<String, List<Object>> key, int count)
    {
        String message = key.getValue().isEmpty() ? key.getKey() : key.getKey().formatted(key.getValue().toArray());
        return FORMAT.formatted(message, count);
    }

    private record ShapingLoggerState(int count, long lastLogTime) {}

    public enum MODE
    {
        FORMAT, FULL
    }
}
