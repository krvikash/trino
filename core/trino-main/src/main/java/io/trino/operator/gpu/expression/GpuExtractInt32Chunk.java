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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.Aggregation128Utils;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Extracts one of the four 32-bit chunks of a DECIMAL128 column. Used to feed a chunked
 * sum aggregation: chunks 0-2 are unsigned (UINT32), chunk 3 is signed (INT32). Each chunk
 * is independently summed as INT64, and {@code Aggregation128Utils.combineInt64SumChunks}
 * reassembles them into DECIMAL128 with overflow detection.
 */
public class GpuExtractInt32Chunk
        implements GpuExpression
{
    private final int chunkIdx;
    private final DType chunkType;

    public GpuExtractInt32Chunk(int chunkIdx, DType chunkType)
    {
        checkArgument(chunkIdx >= 0 && chunkIdx < 4, "chunkIdx must be in [0, 3], got %s", chunkIdx);
        this.chunkIdx = chunkIdx;
        this.chunkType = requireNonNull(chunkType, "chunkType is null");
        DType.DTypeEnum chunkTypeId = chunkType.getTypeId();
        checkArgument(chunkTypeId == DType.DTypeEnum.UINT32 || chunkTypeId == DType.DTypeEnum.INT32,
                "chunkType must be UINT32 or INT32, got %s",
                chunkType);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 1, "Expected exactly one input column, got %s", inputColumns.size());
        @Borrow ColumnVector input = inputColumns.getFirst();
        DType.DTypeEnum inputTypeId = input.getType().getTypeId();
        checkState(inputTypeId == DType.DTypeEnum.DECIMAL128,
                "Expected DECIMAL128 input column, got %s",
                input.getType());
        return Aggregation128Utils.extractInt32Chunk(input, chunkType, chunkIdx);
    }
}
