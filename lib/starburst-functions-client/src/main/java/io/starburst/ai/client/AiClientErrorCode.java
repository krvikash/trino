/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;

public enum AiClientErrorCode
        implements ErrorCodeSupplier
{
    AI_CLIENT_ERROR(0, EXTERNAL),
    INVALID_MODEL_SPEC_PROPERTY(1, INTERNAL_ERROR),
    UNSUPPORTED_MODEL(2, INTERNAL_ERROR),
    INVALID_MODEL_CONFIGURATION(3, EXTERNAL);

    private final ErrorCode errorCode;

    AiClientErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0524_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
