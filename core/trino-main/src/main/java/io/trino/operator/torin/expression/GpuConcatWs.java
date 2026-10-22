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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;

import java.util.List;
import java.util.Objects;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.DType.INT64;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.torin.expression.Fallibility.worst;
import static io.trino.operator.torin.expression.GpuExpressions.allNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.worstOf;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/// `concat_ws(separator, value...)` with a constant, non-NULL separator: the non-NULL values joined
/// by the separator, skipping NULL values, and an empty string when every value is NULL.
/// See `ConcatWsFunction`, the CPU counterpart.
public final class GpuConcatWs
        extends GpuExpression
{
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private final String separator;
    private final List<GpuExpression> values;
    private final int separatorByteLength;

    public GpuConcatWs(String separator, List<GpuExpression> values)
    {
        this.separator = requireNonNull(separator, "separator is null");
        checkArgument(!values.isEmpty(), "concat_ws requires at least 1 value");
        this.values = ImmutableList.copyOf(values);
        this.separatorByteLength = utf8Slice(separator).length();
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        @Own ColumnVector[] columns = new ColumnVector[values.size()];
        try {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = values.get(i).evaluate(positionCount, inputColumns, bindings);
            }
            verifyOutputLength(columns);
            try (Scalar emptyString = Scalar.fromString("")) {
                if (columns.length == 1) {
                    return columns[0].replaceNulls(emptyString);
                }
                try (Scalar separatorScalar = Scalar.fromString(separator)) {
                    // A NULL value becomes an empty string with no separator before it, so it is skipped
                    return ColumnVector.stringConcatenate(separatorScalar, emptyString, columns, false);
                }
            }
        }
        finally {
            closeColumns(columns);
        }
    }

    /// Rejects a batch in which some row would join to more than [#MAX_OUTPUT_LENGTH] bytes,
    /// like `ConcatWsFunction` does on the CPU.
    private void verifyOutputLength(@Borrow ColumnVector[] columns)
    {
        // Every non-NULL value adds its own bytes and one separator, so the sum has one separator too many
        try (Scalar separatorLength = Scalar.fromLong(separatorByteLength);
                Scalar zero = Scalar.fromLong(0);
                ClosingRef<ColumnVector> outputLengths = ClosingRef.own(withSeparatorLength(columns[0], separatorLength, zero))) {
            for (int i = 1; i < columns.length; i++) {
                try (ColumnVector partial = outputLengths.take();
                        ColumnVector next = withSeparatorLength(columns[i], separatorLength, zero)) {
                    outputLengths.set(partial.binaryOp(ADD, next, INT64));
                }
            }
            try (Scalar longest = outputLengths.borrow().max()) {
                if (longest.isValid() && longest.getLong() - separatorByteLength > MAX_OUTPUT_LENGTH) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
                }
            }
        }
    }

    private static @Move ColumnVector withSeparatorLength(@Borrow ColumnVector column, @Borrow Scalar separatorLength, @Borrow Scalar zero)
    {
        // getByteCount - CPU function also caps in bytes
        try (ColumnVector byteCount = column.getByteCount();
                ColumnVector withSeparator = byteCount.binaryOp(ADD, separatorLength, INT64)) {
            return withSeparator.replaceNulls(zero);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        // The length check cannot fail on a row where every value is NULL, as the result is empty there
        if (allNullOnNullInputs(values)) {
            return worst(worstOf(values), FALLIBLE_EXCEPT_ALL_NULLS);
        }
        return FALLIBLE;
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        // The separator is never NULL, and NULL values are skipped
        return false;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuConcatWs other
                && separator.equals(other.separator)
                && values.equals(other.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), separator, values);
    }
}
