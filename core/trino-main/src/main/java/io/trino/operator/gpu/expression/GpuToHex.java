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
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/// Encodes the bytes of a varbinary as uppercase hexadecimal digits, two per byte.
public final class GpuToHex
        extends GpuExpression
{
    private final GpuExpression argument;

    public GpuToHex(GpuExpression argument)
    {
        this.argument = requireNonNull(argument, "argument is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ColumnVector bytes = argument.evaluate(positionCount, inputColumns, bindings);
                ColumnView elements = bytes.getChildColumnView(0)) {
            verify(elements.getType().equals(DType.UINT8), "Unexpected list element type: %s", elements.getType());
            // cuDF toHex renders each UINT8 as exactly two uppercase digits, "00" included
            try (ColumnVector byteDigits = elements.toHex();
                    ColumnView digitLists = bytes.replaceListChild(byteDigits);
                    Scalar empty = Scalar.fromString("")) {
                // A null list stays null, and an empty one becomes the empty string
                return digitLists.stringConcatenateListElements(empty, empty, false, true);
            }
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return argument.fallibility();
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return argument.nullOnNullInputs();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuToHex other && argument.equals(other.argument);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), argument);
    }
}
