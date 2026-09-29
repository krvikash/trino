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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * The cast pairs a persisted {@link Cast} may use, keyed by the source and target {@code Type#getBaseName()}.
 * This registry is the contract and the only place to edit.
 * <p>
 * Each entry asserts that the conversion's meaning does not depend on the session, and carries the version of
 * that belief, stamped as a {@code cast:} key. Two families are permanently excluded, because the session leaks
 * into their result: a pair where exactly one side carries a time zone ({@code timestamp with time zone},
 * {@code time with time zone}), which the session time zone resolves; and a pair between {@code char} and
 * {@code varchar} in either direction, whose implementation and coercion direction the session's char/varchar
 * coercion mode selects.
 * <p>
 * Each pair is versioned separately so that a semantic change invalidates only the definitions that use it. Bump
 * a pair's version when the engine changes what the conversion returns for the same input (rounding, truncation,
 * overflow): a stored definition holds values computed under the old semantics, so readers must skip it rather
 * than match against it. Adding a pair needs no bump, since a reader that does not know its key skips every
 * definition stamped with it.
 * <p>
 * Unsupported pairs are reported as {@code false}, never thrown, so a producer can decline to index a definition.
 */
public final class SupportedCasts
{
    private static final Map<CastPair, Integer> CASTS = ImmutableMap.<CastPair, Integer>builder()
            .put(new CastPair(BIGINT, DECIMAL), 1)
            .put(new CastPair(DECIMAL, DECIMAL), 1)
            .put(new CastPair(INTEGER, BIGINT), 1)
            .put(new CastPair(INTEGER, DECIMAL), 1)
            .put(new CastPair(VARCHAR, VARCHAR), 1)
            .buildOrThrow();

    private SupportedCasts() {}

    public static boolean contains(String sourceBaseName, String targetBaseName)
    {
        return CASTS.containsKey(new CastPair(sourceBaseName, targetBaseName));
    }

    public static int semanticVersion(String sourceBaseName, String targetBaseName)
    {
        Integer version = CASTS.get(new CastPair(sourceBaseName, targetBaseName));
        checkArgument(version != null, "Cast from %s to %s is not supported", sourceBaseName, targetBaseName);
        return version;
    }

    public static Set<CastPair> supported()
    {
        return CASTS.keySet();
    }

    public record CastPair(String sourceBaseName, String targetBaseName)
    {
        public CastPair
        {
            requireNonNull(sourceBaseName, "sourceBaseName is null");
            requireNonNull(targetBaseName, "targetBaseName is null");
        }
    }
}
