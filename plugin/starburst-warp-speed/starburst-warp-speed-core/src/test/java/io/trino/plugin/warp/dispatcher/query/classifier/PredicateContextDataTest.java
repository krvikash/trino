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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateContext;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_RANGES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

public class PredicateContextDataTest
{
    private static final VarcharType VARCHAR_TYPE = VarcharType.createVarcharType(10);
    private static final RegularColumn COLUMN = new RegularColumn("v");

    @Test
    public void testLowerNotEqualIsNotLuceneColumn()
    {
        Slice constant = utf8Slice("bla");
        PredicateContextData predicateContextData = lowerPredicate(
                "$not_equal",
                constant,
                ValueSet.ofRanges(Range.lessThan(VARCHAR_TYPE, constant), Range.greaterThan(VARCHAR_TYPE, constant)));
        assertThat(predicateContextData.isLuceneColumn(COLUMN)).isFalse();
    }

    @Test
    public void testLowerGreaterThanIsLuceneColumn()
    {
        Slice constant = utf8Slice("bla");
        PredicateContextData predicateContextData = lowerPredicate(
                "$greater_than",
                constant,
                ValueSet.ofRanges(Range.greaterThan(VARCHAR_TYPE, constant)));
        assertThat(predicateContextData.isLuceneColumn(COLUMN)).isTrue();
    }

    private static PredicateContextData lowerPredicate(String operator, Slice constant, ValueSet valueSet)
    {
        WarpVariable variable = new WarpVariable(new TestingConnectorColumnHandle(VARCHAR_TYPE, COLUMN.getName()), VARCHAR_TYPE);
        WarpCall lowerCall = new WarpCall("lower", ImmutableList.of(variable), VARCHAR_TYPE);
        WarpCall expression = new WarpCall(operator, ImmutableList.of(lowerCall, new WarpSliceConstant(constant, VARCHAR_TYPE)), BOOLEAN);
        NativeExpression nativeExpression = new NativeExpression(
                PREDICATE_TYPE_STRING_RANGES,
                FunctionType.FUNCTION_TYPE_NONE,
                Domain.create(valueSet, false),
                false,
                false,
                ImmutableList.of(),
                TransformFunction.LOWER);
        WarpExpressionData warpExpressionData = new WarpExpressionData(expression, VARCHAR_TYPE, false, Optional.of(nativeExpression), COLUMN);
        return new PredicateContextData(ImmutableMap.of(expression, new PredicateContext(warpExpressionData)), expression);
    }
}
