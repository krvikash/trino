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
package io.trino.spi.connector;

import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * The current maximum value of a materialized view's {@code incremental_column}, as resolved
 * by the connector from cheap metadata rather than a full scan.
 * <p>
 * A connector reports a native value, not a literal: rendering it is the engine's job because the
 * value has to fit the type the <em>view column</em> declares, which the storage table may not
 * preserve exactly. The engine falls back to the correlated subquery when it cannot.
 */
public sealed interface IncrementalColumnWatermark
{
    /**
     * The storage table holds no rows, so the refresh keeps every source row and injects no
     * predicate. Returning this for a table that does hold rows re-appends all of them, so a
     * connector that cannot establish emptiness must return {@link Unknown}.
     */
    record Empty()
            implements IncrementalColumnWatermark
    {
        private static final Empty INSTANCE = new Empty();

        public static IncrementalColumnWatermark emptyIncrementalColumnWatermark()
        {
            return Empty.INSTANCE;
        }
    }

    /**
     * The exact current maximum of the incremental column, in Trino's internal representation for
     * {@code type}, the type the connector read it as. The refresh keeps only source rows strictly
     * greater than it.
     */
    record Value(Type type, Object value)
            implements IncrementalColumnWatermark
    {
        public Value
        {
            requireNonNull(type, "type is null");
            requireNonNull(value, "value is null");
        }

        public static IncrementalColumnWatermark resolvedIncrementalColumnWatermark(Type type, Object value)
        {
            return new Value(type, value);
        }
    }

    /**
     * No exact value could be resolved cheaply, so the engine injects a correlated subquery
     * instead of a literal. A wrong {@link Value} silently corrupts the refresh, so a connector
     * must fail closed to this whenever exactness cannot be guaranteed.
     */
    record Unknown()
            implements IncrementalColumnWatermark
    {
        private static final Unknown INSTANCE = new Unknown();

        public static IncrementalColumnWatermark unknownIncrementalColumnWatermark()
        {
            return Unknown.INSTANCE;
        }
    }
}
