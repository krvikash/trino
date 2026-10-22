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
package io.trino.operator.torin.expression;

import java.util.List;
import java.util.Optional;

import static io.trino.operator.torin.expression.Fallibility.FALLIBLE;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.torin.expression.Fallibility.INFALLIBLE;
import static io.trino.operator.torin.expression.Fallibility.worst;

public final class GpuExpressions
{
    private GpuExpressions() {}

    /// Fallibility of an operation that never fails by itself.
    public static Fallibility worstOf(List<? extends GpuExpression> operands)
    {
        Fallibility worst = INFALLIBLE;
        for (GpuExpression operand : operands) {
            worst = worst(worst, operand.fallibility());
        }
        return worst;
    }

    /// Fallibility of an operation whose check is skipped when any operand is NULL.
    public static Fallibility nullSkippingCheck(List<? extends GpuExpression> operands)
    {
        boolean anyNull = false;
        for (GpuExpression operand : operands) {
            if (operand.fallibility() == FALLIBLE) {
                return FALLIBLE;
            }
            anyNull |= operand.nullOnNullInputs();
        }
        return anyNull ? FALLIBLE_EXCEPT_ALL_NULLS : FALLIBLE;
    }

    public static boolean anyNullOnNullInputs(List<? extends GpuExpression> operands)
    {
        return operands.stream().anyMatch(GpuExpression::nullOnNullInputs);
    }

    public static boolean allNullOnNullInputs(List<? extends GpuExpression> operands)
    {
        return operands.stream().allMatch(GpuExpression::nullOnNullInputs);
    }

    /// The value of `expression` when it is a non-NULL constant of a type carried as a `long`.
    public static Optional<Long> longValue(GpuExpression expression)
    {
        if (expression instanceof GpuConstant constant && constant.value.orElse(null) instanceof Long value) {
            return Optional.of(value);
        }
        return Optional.empty();
    }
}
