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
package io.trino.plugin.warp;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.plugin.warp.util.TrinoExceptionMapper.WARP_ERROR_CODE_OFFSET;
import static io.trino.spi.ErrorType.EXTERNAL;

public enum WarpErrorCode
        implements ErrorCodeSupplier
{
    WARP_GENERIC(0, EXTERNAL),
    WARP_SETUP(1, EXTERNAL),
    WARP_CONTROL(3, EXTERNAL),
    WARP_ILLEGAL_PARAMETER(5, EXTERNAL),

    // Column/Partition
    WARP_COLUMN_UNKNOWN_PARTITION_COLUMN_TYPE(315, EXTERNAL),

    //    Coordinator/Worker/Nodes
    WARP_CLUSTER_NOT_READY(406, EXTERNAL),
    WARP_DUPLICATE_RECORD(407, EXTERNAL),
    WARP_RULE_CONFIGURATION_ERROR(408, EXTERNAL),
    WARP_EXCEEDED_LOADERS(410, ErrorType.INSUFFICIENT_RESOURCES),

    // storage engine
    WARP_UNRECOVERABLE_COLLECT_FAILED(502, EXTERNAL),
    WARP_ROW_GROUP_ILLEGAL_STATE(505, EXTERNAL),
    WARP_NATIVE_READ_OUT_OF_BOUNDS(506, EXTERNAL),
    WARP_STORAGE_TEMPORARY_ERROR(507, EXTERNAL),
    WARP_STORAGE_PERMANENT_ERROR(508, EXTERNAL),
    WARP_NATIVE_ERROR(509, EXTERNAL),
    WARP_NATIVE_UNRECOVERABLE_ERROR(510, EXTERNAL),
    WARP_UNRECOVERABLE_MATCH_FAILED(511, EXTERNAL),
    WARP_ILLEGAL_BUCKET_CONFIGURATION(512, EXTERNAL),
    WARP_MATCH_FAILED(513, EXTERNAL),
    WARP_FAILED_TO_BUILD_MIXED_PAGE(514, EXTERNAL),
    WARP_MATCH_RANGES_ERROR(515, EXTERNAL),
    WARP_TX_ALLOCATION_FAILED(516, EXTERNAL),
    WARP_TX_ALLOCATION_INTERRUPTED(517, EXTERNAL),
    WARP_PREDICATE_BUFFER_ALLOCATION(518, EXTERNAL),
    WARP_MATCH_COLLECT_ID_ALLOCATION(522, EXTERNAL),

    // WARMUP RULES
    WARP_WARMUP_RULE_UNKNOWN_TABLE(602, EXTERNAL),
    WARP_WARMUP_RULE_UNKNOWN_COLUMN(603, EXTERNAL),
    WARP_WARMUP_RULE_ILLEGAL_CHAR_LENGTH(604, EXTERNAL),
    WARP_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE(605, EXTERNAL),
    WARP_PREDICATE_CACHE_ERROR(606, EXTERNAL),
    WARP_LUCENE_WRITER_ERROR(607, EXTERNAL),
    WARP_LUCENE_FAILURE(608, EXTERNAL),
    WARP_DATA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH(609, EXTERNAL),

    WARP_WARMUP_RULE_ID_NOT_VALID(610, EXTERNAL),
    WARP_INDEX_WARMUP_RULE_IS_NOT_ALLOWED(611, EXTERNAL),
    WARP_WARMUP_OPEN_ERROR(612, EXTERNAL),

    //dictionary
    WARP_DICTIONARY_ERROR(800, EXTERNAL);

    private final ErrorType type;
    private final int code;

    WarpErrorCode(int code, ErrorType type)
    {
        this.type = type;
        this.code = code;
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return new ErrorCode(WARP_ERROR_CODE_OFFSET + code, name(), type);
    }

    public int getCode()
    {
        return code;
    }
}
