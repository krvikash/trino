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
import io.trino.spi.function.CatalogSchemaFunctionName;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The scalar functions a persisted {@link Call} may invoke. This registry is the contract and the only place to edit.
 * <p>
 * Each entry asserts that the function is deterministic and that its result does not depend on the session, and
 * carries the version of that belief, stamped as a {@code fn:} key. Determinism alone is not enough: a
 * deterministic function can still read the session (the session time zone, for example), and an MV storage table
 * holds values computed under the refresh session. Grow the list from measured MV definitions, not speculatively:
 * every entry is permanent surface area in a durable format.
 * <p>
 * Each entry is versioned separately so that a semantic change invalidates only the definitions that use it. Bump
 * an entry's version when the engine changes what the function returns for the same inputs (rounding, overflow,
 * null handling): a stored definition holds values computed under the old semantics, so readers must skip it
 * rather than match against it. Adding an entry needs no bump, since a reader that does not know its key skips
 * every definition stamped with it.
 * <p>
 * Keys are constructed {@link CatalogSchemaFunctionName}s, which normalize to lowercase exactly as
 * {@code BoundSignature#getName()} returns them. Unsupported names are reported as {@code false}, never thrown, so
 * a producer can decline to index a definition.
 */
public final class SupportedFunctions
{
    private static final Map<CatalogSchemaFunctionName, Integer> FUNCTIONS = ImmutableMap.<CatalogSchemaFunctionName, Integer>builder()
            .put(builtin("$operator$EQUAL"), 1)
            .put(builtin("$operator$IDENTICAL"), 1)
            .put(builtin("$operator$LESS_THAN"), 1)
            .put(builtin("$operator$LESS_THAN_OR_EQUAL"), 1)
            .put(builtin("$not"), 1)
            .put(builtin("$operator$ADD"), 1)
            .put(builtin("$operator$SUBTRACT"), 1)
            .put(builtin("$operator$MULTIPLY"), 1)
            .put(builtin("$operator$DIVIDE"), 1)
            .buildOrThrow();

    private SupportedFunctions() {}

    public static boolean contains(CatalogSchemaFunctionName name)
    {
        return FUNCTIONS.containsKey(name);
    }

    public static int semanticVersion(CatalogSchemaFunctionName name)
    {
        Integer version = FUNCTIONS.get(name);
        checkArgument(version != null, "Function %s is not supported", name);
        return version;
    }

    public static Set<CatalogSchemaFunctionName> supported()
    {
        return FUNCTIONS.keySet();
    }

    private static CatalogSchemaFunctionName builtin(String functionName)
    {
        return new CatalogSchemaFunctionName("system", "builtin", functionName);
    }
}
