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
import com.google.common.collect.ImmutableList;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;

public final class GpuStruct
        extends GpuExpression
{
    private final List<GpuExpression> fields;

    public GpuStruct(List<GpuExpression> fields)
    {
        checkArgument(!fields.isEmpty(), "fields is empty");
        this.fields = ImmutableList.copyOf(fields);
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        @Own ColumnVector[] fieldColumns = new ColumnVector[fields.size()];
        try {
            for (int i = 0; i < fields.size(); i++) {
                fieldColumns[i] = fields.get(i).evaluate(positionCount, inputColumns, bindings);
            }
            return ColumnVector.makeStruct(fieldColumns);
        }
        finally {
            closeColumns(fieldColumns);
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        return worstOf(fields);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        return false;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuStruct other
                && fields.equals(other.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), fields);
    }
}
