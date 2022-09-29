/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.exception;

import io.starburst.stargate.buffer.data.client.ErrorCode;

import static java.util.Objects.requireNonNull;

public class DataServerException
        extends RuntimeException
{
    private final ErrorCode errorCode;

    public DataServerException(ErrorCode errorCode, String message)
    {
        super(message);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
    }

    public DataServerException(ErrorCode errorCode, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }
}
