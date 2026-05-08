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
import ai.rapids.cudf.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/**
 * Reassembles four 64-bit chunk sums (least to most significant) into a DECIMAL128 sum and
 * emits the result as a LIST&lt;INT8&gt; column where each row carries the 16 native-byte-order
 * bytes of the sum — bit-compatible with {@code LongDecimalWithOverflowStateSerializer}'s
 * 16-byte VARBINARY form. Throws {@link io.trino.spi.StandardErrorCode#NUMERIC_VALUE_OUT_OF_RANGE}
 * if any group's reassembly overflowed the 128-bit range.
 *
 * <p>cuDF's {@code byte_cast} (which backs {@code asByteList}) does not accept fixed-point
 * inputs, so we cannot run it directly on the DECIMAL128 sum. Instead we re-extract the four
 * 32-bit chunks of the reassembled value, byte-cast each (numeric — supported), and concatenate
 * by row. The bytes land in the same order as cuDF's DECIMAL128 in-memory layout:
 * [chunk0_LE, chunk1_LE, chunk2_LE, chunk3_LE] = [low_8_LE, high_8_LE].
 */
public class GpuCombineSumChunksToVarbinary
        implements GpuExpression
{
    private final DType decimal128Type;

    public GpuCombineSumChunksToVarbinary(DType decimal128Type)
    {
        this.decimal128Type = requireNonNull(decimal128Type, "decimal128Type is null");
        checkState(decimal128Type.getTypeId() == DType.DTypeEnum.DECIMAL128,
                "decimal128Type must be DECIMAL128, got %s", decimal128Type);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, @Borrow List<@Borrow ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 4, "Expected 4 chunk columns, got %s", inputColumns.size());

        try (Table chunks = new Table(
                inputColumns.get(0),
                inputColumns.get(1),
                inputColumns.get(2),
                inputColumns.get(3));
                Table assembled = Aggregation128Utils.combineInt64SumChunks(chunks, decimal128Type)) {
            checkState(assembled.getNumberOfColumns() == 2, "Expected 2-column result, got %s", assembled.getNumberOfColumns());
            if (anyTrue(assembled.getColumn(0))) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return decimal128ToBytes(assembled.getColumn(1));
        }
    }

    /**
     * Convert a DECIMAL128 column to LIST&lt;INT8&gt; with 16 native-byte-order bytes per row.
     * Goes via the four 32-bit chunks because cuDF's byte_cast rejects fixed-point inputs.
     */
    static @Move ColumnVector decimal128ToBytes(@Borrow ColumnVector decimal128)
    {
        try (ColumnVector chunk0 = Aggregation128Utils.extractInt32Chunk(decimal128, DType.UINT32, 0);
                ColumnVector chunk1 = Aggregation128Utils.extractInt32Chunk(decimal128, DType.UINT32, 1);
                ColumnVector chunk2 = Aggregation128Utils.extractInt32Chunk(decimal128, DType.UINT32, 2);
                ColumnVector chunk3 = Aggregation128Utils.extractInt32Chunk(decimal128, DType.INT32, 3);
                // asByteList(false) keeps native byte order, matching cuDF's DECIMAL128 layout.
                ColumnVector bytes0 = chunk0.asByteList(false);
                ColumnVector bytes1 = chunk1.asByteList(false);
                ColumnVector bytes2 = chunk2.asByteList(false);
                ColumnVector bytes3 = chunk3.asByteList(false)) {
            return ColumnVector.listConcatenateByRow(bytes0, bytes1, bytes2, bytes3);
        }
    }
}
