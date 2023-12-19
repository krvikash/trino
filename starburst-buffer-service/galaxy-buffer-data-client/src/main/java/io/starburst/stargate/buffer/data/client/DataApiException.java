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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DataApiException
        extends RuntimeException
{
    private final ErrorCode errorCode;
    private final Optional<RateLimitInfo> rateLimitInfo;

    public DataApiException(ErrorCode errorCode, String message)
    {
        this(errorCode, message, Optional.empty());
    }

    public DataApiException(ErrorCode errorCode, String message, Optional<RateLimitInfo> rateLimitInfo)
    {
        super(message);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.rateLimitInfo = requireNonNull(rateLimitInfo, "rateLimitInfo is null");
    }

    public DataApiException(ErrorCode errorCode, String message, Throwable cause)
    {
        this(errorCode, message, cause, Optional.empty());
    }

    public DataApiException(ErrorCode errorCode, String message, Throwable cause, Optional<RateLimitInfo> rateLimitInfo)
    {
        super(message, cause);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.rateLimitInfo = requireNonNull(rateLimitInfo, "rateLimitInfo is null");
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public Optional<RateLimitInfo> getRateLimitInfo()
    {
        return rateLimitInfo;
    }
}
