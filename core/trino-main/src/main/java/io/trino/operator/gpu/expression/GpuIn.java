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
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.Column;
import io.trino.operator.gpu.GpuTypeConversion.ToColumn;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

public class GpuIn
        implements GpuExpression
{
    private final GpuExpression value;
    private final List<Object> nonNullConstants;
    private final Type type;
    private final ToColumn toColumn;
    private final boolean hasNull;

    public GpuIn(GpuExpression value, List<Object> nonNullConstants, boolean hasNull, Type type, ToColumn toColumn)
    {
        this.value = requireNonNull(value, "value is null");
        this.nonNullConstants = ImmutableList.copyOf(nonNullConstants);
        this.hasNull = hasNull;
        this.type = requireNonNull(type, "type is null");
        this.toColumn = requireNonNull(toColumn, "toColumn is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (@Own ColumnVector valueColumn = value.evaluate(positionCount, inputColumns);
                @Own ColumnVector inList = buildInListColumn()) {
            @Own ColumnVector result = valueColumn.contains(inList);

            if (hasNull) {
                // if the list contains NULL and no match is found, return NULL instead of FALSE
                try (result;
                        @Own Scalar nullScalar = Scalar.fromNull(DType.BOOL8);
                        @Own Scalar falseScalar = Scalar.fromBool(false);
                        @Own ColumnVector isFalse = result.equalTo(falseScalar)) {
                    return isFalse.ifElse(nullScalar, result);
                }
            }

            return result;
        }
    }

    private @Move ColumnVector buildInListColumn()
    {
        BlockBuilder builder = type.createBlockBuilder(null, nonNullConstants.size());
        for (Object constant : nonNullConstants) {
            writeNativeValue(type, builder, constant);
        }
        Block block = builder.build();
        return toColumn.copyToDevice(new Column.Blocks(ImmutableList.of(block)));
    }
}
