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
package io.trino.plugin.base.util;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;

public record MaybeLazy<T>(
        Optional<T> value,
        Optional<Supplier<T>> lazy)
{
    public MaybeLazy
    {
        checkArgument(value.isPresent() != lazy.isPresent(), "Expected exactly one of value, lazy to be present");
    }

    public static <T> MaybeLazy<T> ofValue(T value)
    {
        return new MaybeLazy<>(Optional.of(value), Optional.empty());
    }

    public static <T> MaybeLazy<T> ofLazy(Supplier<T> supplier)
    {
        return new MaybeLazy<>(Optional.empty(), Optional.of(supplier));
    }

    public T getOrCompute()
    {
        return value.orElseGet(() -> lazy.orElseThrow().get());
    }

    public <U> MaybeLazy<U> transform(Function<T, U> function)
    {
        if (value.isPresent()) {
            return MaybeLazy.ofValue(function.apply(value.orElseThrow()));
        }
        return MaybeLazy.ofLazy(() -> function.apply(lazy.orElseThrow().get()));
    }
}
