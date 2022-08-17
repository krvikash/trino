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

public class DataApiException
        extends RuntimeException
{
    private final ErrorCode errorCode;

    public DataApiException(ErrorCode errorCode, String message)
    {
        super(message);
        this.errorCode = errorCode;
    }

    public DataApiException(ErrorCode errorCode, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }
}
