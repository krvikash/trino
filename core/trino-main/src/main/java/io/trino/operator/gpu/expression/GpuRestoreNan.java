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
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.expression.CudfUtils.floatingScalar;
import static java.util.Objects.requireNonNull;

/// Post-projection for floating-point min/max: restores NaN for groups where all non-null values were NaN.
public final class GpuRestoreNan
        extends GpuExpression
{
    private final DType dType;

    public GpuRestoreNan(DType dType)
    {
        this.dType = requireNonNull(dType, "dType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        checkState(inputColumns.size() == 2, "Expected 2 input columns (minMaxResult, hasNan), got %s", inputColumns.size());
        @Borrow ColumnVector minMaxResult = inputColumns.get(0);
        checkState(minMaxResult.getType().equals(dType), "Expected minMaxResult column of type %s, got %s", dType, minMaxResult.getType());
        @Borrow ColumnVector hasNan = inputColumns.get(1);
        checkState(hasNan.getType().equals(DType.BOOL8), "Expected hasNan column of type BOOL8, got %s", hasNan.getType());

        try (Scalar nanScalar = floatingScalar(dType, Double.NaN);
                Scalar nullScalar = floatingScalar(dType, null);
                ColumnVector nanReplacement = hasNan.ifElse(nanScalar, nullScalar)) {
            return minMaxResult.replaceNulls(nanReplacement);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return Fallibility.INFALLIBLE;
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuRestoreNan other && dType.equals(other.dType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), dType);
    }
}
