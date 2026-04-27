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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum BufferServiceExchangeErrorCode
        implements ErrorCodeSupplier
{
    INVALID_TASK_ID(0, INTERNAL_ERROR, true),
    INVALID_ATTEMPT_ID(1, INTERNAL_ERROR, true),
    COMMUNICATION_FAILURE(2, INTERNAL_ERROR, false),
    CONFIGURATION_ERROR(3, USER_ERROR, true);

    private final ErrorCode errorCode;

    BufferServiceExchangeErrorCode(int code, ErrorType type, boolean fatal)
    {
        errorCode = new ErrorCode(code + 0x0900_0000, name(), type, fatal);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
