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
package io.trino.sql.newir;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public final class Attributes
{
    public static final Attributes EMPTY = new Attributes(ImmutableMap.of());

    private final ImmutableMap<AttributeKey, Object> values;

    private Attributes(Map<AttributeKey, Object> values)
    {
        this.values = ImmutableMap.copyOf(requireNonNull(values, "values is null"));
    }

    public static Attributes empty()
    {
        return EMPTY;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Object get(AttributeKey key)
    {
        return values.get(key);
    }

    public boolean isEmpty()
    {
        return values.isEmpty();
    }

    public Set<Map.Entry<AttributeKey, Object>> entrySet()
    {
        return values.entrySet();
    }

    public Attributes filterKeys(Predicate<AttributeKey> predicate)
    {
        Builder builder = builder();
        values.forEach((key, value) -> {
            if (predicate.test(key)) {
                builder.putUnchecked(key, value);
            }
        });
        return builder.buildOrThrow();
    }

    public Partition partitionKeys(Predicate<AttributeKey> predicate)
    {
        Builder matching = builder();
        Builder nonMatching = builder();
        values.forEach((key, value) -> {
            if (predicate.test(key)) {
                matching.putUnchecked(key, value);
            }
            else {
                nonMatching.putUnchecked(key, value);
            }
        });
        return new Partition(matching.buildOrThrow(), nonMatching.buildOrThrow());
    }

    @Override
    public boolean equals(Object object)
    {
        return object instanceof Attributes other && values.equals(other.values);
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }

    @Override
    public String toString()
    {
        return values.toString();
    }

    public record Partition(Attributes matching, Attributes nonMatching)
    {
        public Partition
        {
            matching = requireNonNull(matching, "matching is null").isEmpty()
                    ? EMPTY
                    : new Attributes(matching.values);
            nonMatching = requireNonNull(nonMatching, "nonMatching is null").isEmpty()
                    ? EMPTY
                    : new Attributes(nonMatching.values);
        }
    }

    public static final class Builder
    {
        private final ImmutableMap.Builder<AttributeKey, Object> builder = ImmutableMap.builder();

        private Builder() {}

        public Builder putAll(Attributes attributes)
        {
            builder.putAll(attributes.values);
            return this;
        }

        public Builder putUnchecked(AttributeKey key, Object value)
        {
            builder.put(key, value);
            return this;
        }

        public Attributes buildOrThrow()
        {
            ImmutableMap<AttributeKey, Object> values = builder.buildOrThrow();
            if (values.isEmpty()) {
                return EMPTY;
            }
            return new Attributes(values);
        }

        public Attributes buildKeepingLast()
        {
            ImmutableMap<AttributeKey, Object> values = builder.buildKeepingLast();
            if (values.isEmpty()) {
                return EMPTY;
            }
            return new Attributes(values);
        }
    }
}
