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
package io.trino.plugin.warp.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class WarpExpressionData
{
    private final WarpExpression expression;

    private final RegularColumn warpColumn;
    private final Type columnType;
    private final boolean collectNulls;
    private final boolean isAll;
    private final Optional<NativeExpression> nativeExpressionOptional;

    @JsonCreator
    public WarpExpressionData(@JsonProperty("expression") WarpExpression expression,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("collectNulls") boolean collectNulls,
            @JsonProperty("nativeExpressionOptional") Optional<NativeExpression> nativeExpressionOptional,
            @JsonProperty("warpColumn") RegularColumn warpColumn)
    {
        this.expression = requireNonNull(expression);
        this.columnType = requireNonNull(columnType);
        this.collectNulls = collectNulls || (nativeExpressionOptional.isPresent() && nativeExpressionOptional.get().collectNulls());
        this.isAll = calcIsAll(expression);
        this.warpColumn = requireNonNull(warpColumn);
        this.nativeExpressionOptional = nativeExpressionOptional;
    }

    @JsonProperty("expression")
    public WarpExpression getExpression()
    {
        return expression;
    }

    @JsonProperty("columnType")
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty("collectNulls")
    public boolean isCollectNulls()
    {
        return collectNulls;
    }

    public boolean isAll()
    {
        return isAll;
    }

    @JsonProperty("nativeExpressionOptional")
    public Optional<NativeExpression> getNativeExpressionOptional()
    {
        return nativeExpressionOptional;
    }

    @JsonProperty("warpColumn")
    public RegularColumn getWarpColumn()
    {
        return warpColumn;
    }

    private boolean calcIsAll(WarpExpression expression)
    {
        return expression instanceof WarpConstant &&
                expression.getType() == BooleanType.BOOLEAN &&
                Boolean.parseBoolean(String.valueOf(((WarpConstant) expression).getValue()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarpExpressionData that = (WarpExpressionData) o;
        return Objects.equals(expression, that.getExpression()) &&
                Objects.equals(columnType, that.getColumnType()) &&
                Objects.equals(warpColumn, that.getWarpColumn()) &&
                Objects.equals(nativeExpressionOptional, that.getNativeExpressionOptional()) &&
                collectNulls == that.isCollectNulls();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, collectNulls, columnType, nativeExpressionOptional, warpColumn);
    }

    @Override
    public String toString()
    {
        return "VaradaExpressionData{" +
                "expression=" + expression +
                ", warpColumn=" + warpColumn +
                ", type=" + columnType +
                ", isAll=" + isAll +
                ", nativeExpressionOptional=" + nativeExpressionOptional +
                ", collectNulls=" + collectNulls +
                '}';
    }
}
