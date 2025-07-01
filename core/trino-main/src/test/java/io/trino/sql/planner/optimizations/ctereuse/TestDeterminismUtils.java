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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static org.assertj.core.api.Assertions.assertThat;

class TestDeterminismUtils
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM_FUNCTION = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());
    private static final ResolvedFunction COUNT_FUNCTION = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());

    @Test
    public void testIsDeterministic()
    {
        assertThat(isDeterministic(new Call("%0", ImmutableList.of(), RANDOM_FUNCTION, ImmutableList.of()))).isFalse();
        assertThat(isDeterministic(new Call("%0", ImmutableList.of(), COUNT_FUNCTION, ImmutableList.of()))).isTrue();
    }

    @Test
    public void testDeeplyNestedNonDeterministicOperation()
    {
        // %outerLambdaParameter -> (%intermediateLambdaParameter -> (%innerLambdaParameter -> random()))
        Call randomFunctionCall = new Call("%3", ImmutableList.of(), RANDOM_FUNCTION, ImmutableList.of());

        Lambda innerLambda = new Lambda(
                "%2",
                new Block(
                        Optional.of("^innerLambdaBody"),
                        ImmutableList.of(new Block.Parameter("%innerLambdaParameter", irType(anonymousRow(BIGINT)))),
                        ImmutableList.of(
                                randomFunctionCall,
                                new Return("%4", randomFunctionCall.result(), randomFunctionCall.attributes()))));

        Lambda intermediateLambda = new Lambda(
                "%1",
                new Block(
                        Optional.of("^intermediateLambdaBody"),
                        ImmutableList.of(new Block.Parameter("%intermediateLambdaParameter", irType(anonymousRow(BIGINT)))),
                        ImmutableList.of(
                                innerLambda,
                                new Return("%5", innerLambda.result(), innerLambda.attributes()))));

        Lambda outerLambda = new Lambda(
                "%0",
                new Block(
                        Optional.of("^outerLambdaBody"),
                        ImmutableList.of(new Block.Parameter("%outerLambdaParameter", irType(anonymousRow(BIGINT)))),
                        ImmutableList.of(
                                intermediateLambda,
                                new Return("%6", intermediateLambda.result(), intermediateLambda.attributes()))));

        assertThat(isDeterministic(outerLambda)).isFalse();
    }
}
