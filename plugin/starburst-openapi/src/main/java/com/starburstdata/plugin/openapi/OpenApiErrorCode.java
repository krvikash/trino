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
package com.starburstdata.plugin.openapi;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;

public enum OpenApiErrorCode
        implements ErrorCodeSupplier
{
    OPENAPI_UNEXPECTED_RESPONSE_SCHEMA(0, EXTERNAL),
    OPENAPI_AMBIGUOUS_REFERENCE(1, INTERNAL_ERROR),
    OPENAPI_UNSUPPORTED_PARAMETER(2, INTERNAL_ERROR),
    OPENAPI_GENERIC_EXTERNAL_ERROR(3, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    OpenApiErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0526_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
