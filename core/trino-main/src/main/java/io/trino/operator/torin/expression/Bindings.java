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
import io.trino.spi.torin.borrow.Borrow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;

/// Active LET values
public final class Bindings
{
    private final Map<Integer, @Borrow ColumnVector> bindings = new HashMap<>();

    void put(int bindingId, @Borrow ColumnVector value)
    {
        requireNonNull(value, "value is null");
        verify(bindings.putIfAbsent(bindingId, value) == null, "Binding already exists: %s", bindingId);
    }

    void remove(int bindingId)
    {
        @SuppressWarnings("resource") @Borrow ColumnVector removed = bindings.remove(bindingId);
        verify(removed != null, "Unknown binding: %s", bindingId);
    }

    @Borrow
    ColumnVector binding(int bindingId)
    {
        return verifyNotNull(bindings.get(bindingId), "Unknown binding: %s", bindingId);
    }

    List<Entry<Integer, @Borrow ColumnVector>> allBindings()
    {
        return bindings.entrySet().stream()
                .map(entry -> entry(entry.getKey(), entry.getValue())) // defensive copy
                .collect(toImmutableList());
    }
}
