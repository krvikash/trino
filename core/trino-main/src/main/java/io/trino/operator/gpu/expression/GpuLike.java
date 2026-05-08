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
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Character.isSurrogate;
import static java.lang.Character.isValidCodePoint;
import static java.util.Objects.requireNonNull;

public class GpuLike
        implements GpuExpression
{
    private final GpuExpression searched;
    private final String pattern;
    private final String escape;

    public GpuLike(GpuExpression searched, String pattern, Optional<Character> escape)
    {
        this.searched = requireNonNull(searched, "searched is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.escape = escape.map(Object::toString)
                .orElseGet(() -> {
                    Set<Integer> patternCodePoints = pattern.codePoints().distinct().boxed().collect(toImmutableSet());
                    for (int c = 0; c <= Character.MAX_VALUE; c++) {
                        if (isValidCodePoint(c) && !patternCodePoints.contains(c) && !isSurrogate((char) c)) {
                            return String.valueOf((char) c);
                        }
                    }
                    throw new IllegalArgumentException("Cannot find unused character to dummy escape for pattern: [%s]".formatted(pattern));
                });
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector searched = this.searched.evaluate(positionCount, inputColumns);
                // TODO (https://starburstdata.atlassian.net/browse/ENG-9846) should these Scalars be reused between calls?
                Scalar patternScalar = Scalar.fromString(pattern);
                Scalar escapeScalar = Scalar.fromString(escape)) {
            return searched.like(patternScalar, escapeScalar);
        }
    }
}
