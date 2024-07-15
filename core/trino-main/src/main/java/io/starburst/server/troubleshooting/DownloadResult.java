/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import jakarta.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

public class DownloadResult
{
    @Nullable
    private final InputStream inputStream;
    @Nullable
    private final Exception exception;

    private DownloadResult(@Nullable InputStream inputStream, @Nullable Exception exception)
    {
        checkArgument(inputStream != null ^ exception != null, "either inputStream or exception must be not null, but not both");
        this.inputStream = inputStream;
        this.exception = exception;
    }

    public static DownloadResult ofInputStream(InputStream inputStream)
    {
        return new DownloadResult(inputStream, null);
    }

    public static DownloadResult ofException(Exception e)
    {
        return new DownloadResult(null, e);
    }

    public boolean isSuccessful()
    {
        return inputStream != null;
    }

    public InputStream inputStream()
    {
        if (isSuccessful()) {
            return requireNonNull(inputStream, "inputStream is null");
        }
        return new ByteArrayInputStream(getStackTrace(requireNonNull(exception, "exception is null")).getBytes(UTF_8));
    }
}
