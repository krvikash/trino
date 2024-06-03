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
package io.trino.plugin.warp.dispatcher.query;

import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.expression.DomainExpression;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.isInversePredicate;
import static java.util.Objects.requireNonNull;

public class PredicateContext
{
    private final WarpExpressionData warpExpressionData;
    private final boolean isSimplified;

    public PredicateContext(WarpExpressionData warpExpressionData)
    {
        this(requireNonNull(warpExpressionData), false);
    }

    public PredicateContext(WarpExpressionData warpExpressionData, boolean isSimplified)
    {
        this.warpExpressionData = warpExpressionData;
        this.isSimplified = isSimplified;
    }

    public RegularColumn getWarpColumn()
    {
        return warpExpressionData.getWarpColumn();
    }

    public Type getColumnType()
    {
        return warpExpressionData.getColumnType();
    }

    public Domain getDomain()
    {
        return getNativeExpression().map(NativeExpression::domain)
                .orElse(Domain.all(getColumnType()));
    }

    public WarpExpressionData getVaradaExpressionData()
    {
        return warpExpressionData;
    }

    public Optional<NativeExpression> getNativeExpression()
    {
        return warpExpressionData.getNativeExpressionOptional();
    }

    public boolean isCollectNulls()
    {
        return warpExpressionData.isCollectNulls();
    }

    public boolean isInverseWithNulls()
    {
        return warpExpressionData.isCollectNulls() &&
                getExpression() instanceof DomainExpression domainExpression &&
                domainExpression.getDomain().getValues() instanceof SortedRangeSet sortedRangeSet &&
                isInversePredicate(sortedRangeSet, domainExpression.getType());
    }

    public boolean isSimplified()
    {
        return isSimplified;
    }

    public WarpExpression getExpression()
    {
        return warpExpressionData.getExpression();
    }

    public boolean canMergeExpressions(PredicateContext other)
    {
        boolean res = false;
        if (getNativeExpression().isPresent() &&
                other.getNativeExpression().isPresent()) {
            NativeExpression expression1 = getNativeExpression().get();
            NativeExpression expression2 = other.getNativeExpression().get();
            if (expression1.functionType() == expression2.functionType() &&
                    Objects.equals(expression1.transformFunction(), expression2.transformFunction()) &&
                    expression1.functionParams().equals(expression2.functionParams())) {
                res = true;
            }
        }
        return res;
    }

    public TransformFunction getTransformedColumn()
    {
        return getNativeExpression().isPresent() ? getNativeExpression().get().transformFunction() : TransformFunction.NONE;
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
        PredicateContext that = (PredicateContext) o;
        return isSimplified == that.isSimplified() &&
                Objects.equals(warpExpressionData, that.warpExpressionData);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(warpExpressionData, isSimplified);
    }

    @Override
    public String toString()
    {
        return "PredicateContext{" +
                "isSimplified=" + isSimplified +
                ", varadaExpressionData=" + warpExpressionData +
                '}';
    }
}
