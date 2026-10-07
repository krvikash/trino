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
 * The aggregate functions an {@link Aggregation} may persist. Like {@link SupportedFunctions}, each entry asserts that
 * the function's result does not depend on the session, because the storage table holds values computed under the
 * refresh session, and carries the version of that belief, stamped as a {@code fn:} key.
 * <p>
 * {@code array_agg} makes {@link Aggregation.OrderingScheme} reachable: the planner drops {@code ORDER BY} for
 * order-insensitive functions. Without {@code ORDER BY} its element order is unspecified, so the stored order is a
 * valid answer.
 */
public final class SupportedAggregateFunctions
{
    private static final Map<CatalogSchemaFunctionName, Integer> FUNCTIONS = ImmutableMap.<CatalogSchemaFunctionName, Integer>builder()
            .put(builtin("array_agg"), 1)
            .put(builtin("avg"), 1)
            .put(builtin("count"), 1)
            .put(builtin("max"), 1)
            .put(builtin("min"), 1)
            .put(builtin("sum"), 1)
            .buildOrThrow();

    private SupportedAggregateFunctions() {}

    public static boolean contains(CatalogSchemaFunctionName name)
    {
        return FUNCTIONS.containsKey(name);
    }

    public static int semanticVersion(CatalogSchemaFunctionName name)
    {
        Integer version = FUNCTIONS.get(name);
        checkArgument(version != null, "Aggregate function %s is not supported", name);
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
