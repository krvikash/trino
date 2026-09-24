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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.GpuExpressions.anyNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.nullSkippingCheck;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

public final class GpuConcat
        extends GpuExpression
{
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private final List<GpuExpression> arguments;

    public GpuConcat(List<GpuExpression> arguments)
    {
        checkArgument(arguments.size() >= 2, "concat requires at least 2 arguments, got %s", arguments.size());
        this.arguments = ImmutableList.copyOf(arguments);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        @Own ColumnVector[] columns = new ColumnVector[arguments.size()];
        try {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = arguments.get(i).evaluate(positionCount, inputColumns, bindings);
            }
            verifyOutputLength(columns);
            return ColumnVector.stringConcatenate(columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    /// Rejects a batch in which some row would concatenate to more than [#MAX_OUTPUT_LENGTH] bytes,
    /// like `ConcatFunction` does on the CPU.
    private static void verifyOutputLength(@Borrow ColumnVector[] columns)
    {
        // getByteCount - CPU function also cap in bytes
        try (ClosingRef<ColumnVector> outputLengths = ClosingRef.own(columns[0].getByteCount())) {
            for (int i = 1; i < columns.length; i++) {
                try (ColumnVector partial = outputLengths.take();
                        ColumnVector next = columns[i].getByteCount()) {
                    outputLengths.set(partial.binaryOp(ADD, next, INT64));
                }
            }
            // Null rows reduce away, matching the CPU, which never concatenates a row with a null argument.
            // There are always at least two arguments, so the loop ran and the lengths are INT64 by now.
            try (Scalar longest = outputLengths.borrow().max()) {
                if (longest.isValid() && longest.getLong() > MAX_OUTPUT_LENGTH) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
                }
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return nullSkippingCheck(arguments);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return anyNullOnNullInputs(arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuConcat other && arguments.equals(other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), arguments);
    }
}
