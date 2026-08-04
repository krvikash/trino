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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class Apply
        extends GpuExpression
{
    private final int channel;
    private final GpuExpression expression;

    public Apply(int channel, GpuExpression expression)
    {
        checkArgument(channel >= 0, "channel must be non-negative: %s", channel);
        this.channel = channel;
        this.expression = requireNonNull(expression, "expression is null");
    }

    @Override
    public ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        return expression.evaluate(positionCount, ImmutableList.of(inputColumns.get(channel)));
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof Apply other
                && channel == other.channel
                && expression.equals(other.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), channel, expression);
    }
}
