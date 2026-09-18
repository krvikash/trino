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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTrinoOperation
{
    @Test
    public void testReplaceAllArguments()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Value first = new Operation.Result("%first", irType(BIGINT));
        Value second = new Operation.Result("%second", irType(BIGINT));
        In in = new In("%in", input, ImmutableList.of(first, second), TrinoOperation.emptySourceAttributes(3));

        Value newInput = new Operation.Result("%new_input", irType(BIGINT));
        Value newFirst = new Operation.Result("%new_first", irType(BIGINT));
        Value newSecond = new Operation.Result("%new_second", irType(BIGINT));
        assertThat(in.withArguments(ImmutableList.of(newInput, newFirst, newSecond)).arguments())
                .containsExactly(newInput, newFirst, newSecond);

        // The index-based method remains available for replacing a single argument.
        assertThat(in.withArgument(newFirst, 1).arguments())
                .containsExactly(input, newFirst, second);
    }

    @Test
    public void testReplaceAllArgumentsValidation()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Value listValue = new Operation.Result("%list_value", irType(BIGINT));
        In in = new In("%in", input, ImmutableList.of(listValue), TrinoOperation.emptySourceAttributes(2));

        assertThatThrownBy(() -> in.withArguments(ImmutableList.of(input)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("arguments list size mismatch");

        Value booleanValue = new Operation.Result("%boolean", irType(BOOLEAN));
        assertThatThrownBy(() -> in.withArguments(ImmutableList.of(input, booleanValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("argument type mismatch at index 1");
    }

    @Test
    public void testDefaultReplaceAllArguments()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Return returnOperation = new Return("%return", input, Attributes.empty());
        Value newInput = new Operation.Result("%new_input", irType(BIGINT));

        assertThat(returnOperation.withArguments(ImmutableList.of(newInput)).arguments())
                .containsExactly(newInput);
    }
}
