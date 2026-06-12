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
package io.trino.sql.planner.exploratory.rule;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record Result<T>(Optional<T> newValue)
{
    private static final Result<?> UNCHANGED = new Result<>(Optional.empty());

    public Result
    {
        requireNonNull(newValue, "newValue is null");
    }

    public static <T> Result<T> of(T result)
    {
        return new Result<>(Optional.of(result));
    }

    public static <T> Result<T> unchanged()
    {
        @SuppressWarnings("unchecked")
        Result<T> unchanged = (Result<T>) UNCHANGED;
        return unchanged;
    }

    public boolean isPresent()
    {
        return newValue.isPresent();
    }

    public T get()
    {
        return newValue.orElseThrow();
    }
}
