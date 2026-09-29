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

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * The single source of truth for IR version stamps.
 * <p>
 * A stored definition is readable iff every name it was stamped with is present in {@link #SUPPORTED} at the
 * same version. {@link #collect} stamps a tree with the union of its nodes' {@link IrNode#versionEntries()}, so
 * every wire-format decision the decoder makes must have a name. Names live in disjoint namespaces:
 * <ul>
 * <li>bare: an {@link Operation} node type, grandfathered unprefixed ({@code TableScan})</li>
 * <li>{@value #EXPRESSION_PREFIX}: an {@link Expression} node type ({@code expr:Reference})</li>
 * <li>{@value #FUNCTION_PREFIX}: one {@link SupportedFunctions} entry's semantics, keyed by its
 * {@code catalog.schema.function} name ({@code fn:system.builtin.$operator$equal})</li>
 * <li>{@value #CAST_PREFIX}: one {@link SupportedCasts} pair's conversion semantics ({@code cast:varchar->varchar})</li>
 * <li>{@value #LEAF_PREFIX}: a shared leaf serialized form, stamped by {@code Expression} nodes only
 * ({@code leaf:Symbol}, {@code leaf:Type}, {@code leaf:BoundSignature})</li>
 * </ul>
 * Names are permanent: renaming one silently invalidates every stored definition that uses it.
 */
public final class IrVersions
{
    public static final String EXPRESSION_PREFIX = "expr:";
    public static final String FUNCTION_PREFIX = "fn:";
    public static final String CAST_PREFIX = "cast:";
    public static final String LEAF_PREFIX = "leaf:";

    // A leaf version covers that leaf's JSON form, not its Java API: bump it when a field is added, removed or renamed,
    // or a value's encoding changes. A Symbol or Type change also bumps other keys; see the README's version-bump table.
    public static final String SYMBOL_LEAF = LEAF_PREFIX + "Symbol";
    public static final int SYMBOL_LEAF_VERSION = 1;
    public static final String TYPE_LEAF = LEAF_PREFIX + "Type";
    public static final int TYPE_LEAF_VERSION = 1;
    public static final String BOUND_SIGNATURE_LEAF = LEAF_PREFIX + "BoundSignature";
    public static final int BOUND_SIGNATURE_LEAF_VERSION = 1;

    /**
     * {@code Operation} versions, keyed by the grandfathered bare node name.
     */
    public static final Map<String, Integer> OPERATION_VERSIONS = ImmutableMap.of(
            Output.NAME, Output.VERSION,
            TableScan.NAME, TableScan.VERSION);

    /**
     * {@code Expression} versions, keyed by the {@value #EXPRESSION_PREFIX} namespaced node name.
     */
    public static final Map<String, Integer> EXPRESSION_VERSIONS = ImmutableMap.of(
            expressionKey(Call.NAME), Call.VERSION,
            expressionKey(Cast.NAME), Cast.VERSION,
            expressionKey(FieldReference.NAME), FieldReference.VERSION,
            expressionKey(In.NAME), In.VERSION,
            expressionKey(IsNull.NAME), IsNull.VERSION,
            expressionKey(Logical.NAME), Logical.VERSION,
            expressionKey(Reference.NAME), Reference.VERSION);

    /**
     * {@link SupportedFunctions} versions, keyed by {@value #FUNCTION_PREFIX}{@code <catalog>.<schema>.<function>}.
     */
    public static final Map<String, Integer> FUNCTION_VERSIONS = SupportedFunctions.supported().stream()
            .collect(toImmutableMap(IrVersions::functionKey, SupportedFunctions::semanticVersion));

    /**
     * {@link SupportedCasts} versions, keyed by {@value #CAST_PREFIX}{@code <source base type>-><target base type>}.
     */
    public static final Map<String, Integer> CAST_VERSIONS = SupportedCasts.supported().stream()
            .collect(toImmutableMap(
                    pair -> castKey(pair.sourceBaseName(), pair.targetBaseName()),
                    pair -> SupportedCasts.semanticVersion(pair.sourceBaseName(), pair.targetBaseName())));

    public static final Map<String, Integer> LEAF_VERSIONS = ImmutableMap.of(
            SYMBOL_LEAF, SYMBOL_LEAF_VERSION,
            TYPE_LEAF, TYPE_LEAF_VERSION,
            BOUND_SIGNATURE_LEAF, BOUND_SIGNATURE_LEAF_VERSION);

    /**
     * Every name this build can read, with the version it reads it at.
     */
    public static final Map<String, Integer> SUPPORTED = ImmutableMap.<String, Integer>builder()
            .putAll(OPERATION_VERSIONS)
            .putAll(EXPRESSION_VERSIONS)
            .putAll(FUNCTION_VERSIONS)
            .putAll(CAST_VERSIONS)
            .putAll(LEAF_VERSIONS)
            .buildOrThrow();

    private IrVersions() {}

    static String expressionKey(String nodeName)
    {
        return EXPRESSION_PREFIX + nodeName;
    }

    static String functionKey(CatalogSchemaFunctionName name)
    {
        return FUNCTION_PREFIX + name;
    }

    static String castKey(String sourceBaseName, String targetBaseName)
    {
        return CAST_PREFIX + sourceBaseName + "->" + targetBaseName;
    }

    /**
     * Returns the names, with versions, that {@code root}'s tree uses.
     */
    public static Map<String, Integer> collect(IrNode root)
    {
        Map<String, Integer> versions = new HashMap<>();
        collect(root, versions);
        return ImmutableMap.copyOf(versions);
    }

    private static void collect(IrNode node, Map<String, Integer> versions)
    {
        node.versionEntries().forEach((name, version) -> {
            Integer previous = versions.putIfAbsent(name, version);
            checkState(previous == null || previous.equals(version), "Name %s is stamped with versions %s and %s", name, previous, version);
        });
        for (IrNode child : node.children()) {
            collect(child, versions);
        }
    }
}
