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

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * The single source of truth for IR version stamps.
 * <p>
 * A stored definition is readable iff every name it was stamped with is present in {@link #SUPPORTED} at the
 * same version. {@link #collect} stamps a tree with the union of its nodes' {@link IrNode#versionEntries()}.
 */
public final class IrVersions
{
    /**
     * {@code Operation} versions, keyed by the grandfathered bare node name.
     */
    public static final Map<String, Integer> OPERATION_VERSIONS = ImmutableMap.of(
            Output.NAME, Output.VERSION,
            TableScan.NAME, TableScan.VERSION);

    /**
     * Every name this build can read, with the version it reads it at.
     */
    public static final Map<String, Integer> SUPPORTED = ImmutableMap.<String, Integer>builder()
            .putAll(OPERATION_VERSIONS)
            .buildOrThrow();

    private IrVersions() {}

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
