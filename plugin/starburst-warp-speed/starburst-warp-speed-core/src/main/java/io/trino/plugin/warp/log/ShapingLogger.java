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
import com.google.errorprone.annotations.FormatString;
import io.airlift.log.Logger;
import io.trino.plugin.warp.tools.util.Pair;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.dispatcher.WarpMDCContext.CATALOG_NAME_LOCAL_PROPERTY;
import static io.trino.plugin.warp.dispatcher.WarpMDCContext.QUERY_ID_LOCAL_PROPERTY;
import static java.util.Objects.requireNonNull;

/**
 * this Logger counts similar logs and only outputs a single log message based on threshold / intervalInMillis
 */
public class ShapingLogger
{
    private static final String CATALOG_FORMAT = "catalog[%s]: ";
    private static final String QUERY_FORMAT = CATALOG_FORMAT + "queryId[%s]: ";
    private final Map<Pair<String, List<Object>>, ShapingLoggerState> shapingLoggerStateMap = new ConcurrentHashMap<>();

    private final String catalog;
    private final Logger logger;

    // threshold for log flushing. if threshold<=0 then threshold is ignored
    private final int threshold;

    // interval in millis for log flushing. if duration = Duration.ZERO then it is ignored
    long durationMillis;

    // if shapeMode=FORMAT then we accumulate by format string
    // if shapeMode=FULL then we accumulate by formatted string
    private final MODE mode;

    // if true first log message will be logged regardless of interval/duration
    private final int numberOfSamples;

    public ShapingLogger(
            String catalog,
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamples,
            MODE mode)
    {
        this.catalog = requireNonNull(catalog);
        this.logger = requireNonNull(logger);
        this.threshold = threshold;
        durationMillis = (duration != null ? duration : Duration.ZERO).toMillis();
        this.numberOfSamples = numberOfSamples;
        checkArgument(threshold == 0 || threshold > numberOfSamples, "threshold must be greater than number of samples");
        this.mode = requireNonNull(mode);
    }

    public void info(String message)
    {
        info("%s", message);
    }

    @FormatMethod
    public void info(@FormatString final String format, Object... args)
    {
        if (logger.isInfoEnabled()) {
            Object[] newArgs = appendProperties(args);
            final String newFormat = appendFormat(format, newArgs.length - args.length);
            log(getKey(newFormat, newArgs), () -> logger.info(newFormat.formatted(newArgs)));
        }
    }

    @FormatMethod
    public void debug(@FormatString final String format, Object... args)
    {
        if (logger.isDebugEnabled()) {
            Object[] newArgs = appendProperties(args);
            String newFormat = appendFormat(format, newArgs.length - args.length);
            log(getKey(newFormat, newArgs), () -> logger.debug(newFormat.formatted(newArgs)));
        }
    }

    public void warn(String message)
    {
        warn("%s", message);
    }

    @FormatMethod
    public void warn(@FormatString final String format, Object... args)
    {
        Object[] newArgs = appendProperties(args);
        String newFormat = appendFormat(format, newArgs.length - args.length);
        log(getKey(newFormat, newArgs), () -> {
            logger.warn(newFormat.formatted(newArgs));
        });
    }

    @FormatMethod
    public void warn(Throwable exception, @FormatString final String format, Object... args)
    {
        Object[] newArgs = appendProperties(args);
        String newFormat = appendFormat(format, newArgs.length - args.length);
        log(getKey(newFormat, newArgs), () -> {
            logger.warn(exception, newFormat.formatted(newArgs));
        });
    }

    public void error(String message)
    {
        error("%s", message);
    }

    @FormatMethod
    public void error(@FormatString final String format, Object... args)
    {
        Object[] newArgs = appendProperties(args);
        String newFormat = appendFormat(format, newArgs.length - args.length);
        log(getKey(newFormat, newArgs), () -> {
            logger.error(newFormat.formatted(newArgs));
        });
    }

    @FormatMethod
    public void error(Throwable e, @FormatString final String format, Object... args)
    {
        Object[] newArgs = appendProperties(args);
        String newFormat = appendFormat(format, newArgs.length - args.length);
        log(getKey(newFormat, newArgs), () -> {
            logger.error(e, newFormat.formatted(newArgs));
        });
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
        return "%s - skipped %d times".formatted(message, count);
    }

    private record ShapingLoggerState(int count, long lastLogTime) {}

    private Object[] appendProperties(Object... args)
    {
        String queryId = MDC.get(QUERY_ID_LOCAL_PROPERTY);
        String threadCatalog = MDC.get(CATALOG_NAME_LOCAL_PROPERTY);

        int additionalArgs = queryId != null ? 2 : 1;
        Object[] newArgs = new Object[args.length + additionalArgs];
        newArgs[0] = threadCatalog != null ? threadCatalog : catalog;
        if (queryId != null) {
            newArgs[1] = queryId;
        }
        System.arraycopy(args, 0, newArgs, additionalArgs, args.length);
        return newArgs;
    }

    private String appendFormat(final String format, int additionalArgs)
    {
        return switch (additionalArgs) {
            case 1 -> CATALOG_FORMAT + format;
            case 2 -> QUERY_FORMAT + format;
            default -> format;
        };
    }

    public enum MODE
    {
        FORMAT, FULL
    }
}
