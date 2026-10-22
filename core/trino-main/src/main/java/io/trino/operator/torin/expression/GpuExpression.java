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

import ai.rapids.cudf.ColumnVector;
import com.google.common.base.Supplier;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;

import java.util.List;

import static com.google.common.base.Suppliers.memoize;

public abstract class GpuExpression
{
    // Derived properties are cached so parents can query both without repeatedly traversing subtrees.
    private final Supplier<Fallibility> fallibility = memoize(this::computeFallibility);
    private final Supplier<Boolean> nullOnNullInputs = memoize(this::computeNullOnNullInputs);

    public abstract @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings);

    /// Whether [#evaluate] can raise a runtime error, as a property of this GPU expression tree: a GPU
    /// node may fail where its CPU counterpart does not, and the other way around.
    public final Fallibility fallibility()
    {
        return fallibility.get();
    }

    protected abstract Fallibility computeFallibility();

    /// Whether the result is guaranteed to be NULL on a row where every column in `inputColumns` and `bindings`
    /// is NULL. `false` only means the result is not known to be NULL there, not that it is guaranteed to be non-NULL.
    public final boolean nullOnNullInputs()
    {
        return nullOnNullInputs.get();
    }

    protected abstract boolean computeNullOnNullInputs();

    /// Two GPU expressions are equal when they are semantically equivalent,
    /// meaning they will produce the same output for the same input columns and bindings.
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();
}
