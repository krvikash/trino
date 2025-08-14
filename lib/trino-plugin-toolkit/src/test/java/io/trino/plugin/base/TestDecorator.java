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
package io.trino.plugin.base;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestDecorator
{
    @Test
    public void testPriorities()
    {
        record Tuple(int payload, Optional<Tuple> next) {}

        List<Decorator<Tuple>> tupleDecorators = IntStream.of(9, 0, 8, 1, 7, 2, 6, 3, 5, 4)
                .mapToObj(priority -> Decorator.withPriority(
                        priority,
                        (Tuple delegate) -> new Tuple(priority, Optional.of(delegate))))
                .collect(toImmutableList());

        Tuple result = Decorator.combine(() -> new Tuple(Integer.MAX_VALUE, Optional.empty()), tupleDecorators);

        Tuple tupleFinal = new Tuple(Integer.MAX_VALUE, Optional.empty());
        Tuple tuple9 = new Tuple(9, Optional.of(tupleFinal));
        Tuple tuple8 = new Tuple(8, Optional.of(tuple9));
        Tuple tuple7 = new Tuple(7, Optional.of(tuple8));
        Tuple tuple6 = new Tuple(6, Optional.of(tuple7));
        Tuple tuple5 = new Tuple(5, Optional.of(tuple6));
        Tuple tuple4 = new Tuple(4, Optional.of(tuple5));
        Tuple tuple3 = new Tuple(3, Optional.of(tuple4));
        Tuple tuple2 = new Tuple(2, Optional.of(tuple3));
        Tuple tuple1 = new Tuple(1, Optional.of(tuple2));
        Tuple tuple0 = new Tuple(0, Optional.of(tuple1));
        assertThat(result).isEqualTo(tuple0);
    }
}
