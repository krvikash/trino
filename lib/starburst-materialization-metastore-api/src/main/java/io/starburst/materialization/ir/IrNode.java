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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;

/**
 * A node of a persisted materialization IR tree.
 */
public sealed interface IrNode
        permits Expression, Operation
{
    /**
     * Stable wire discriminator: the JSON {@code @type} value. Never rename.
     */
    String name();

    /**
     * Stamped under this node's name. Bump it when the node's own JSON payload changes (a field added, removed, renamed
     * or re-encoded). A change inside a function or cast it references bumps that key instead; a change inside a leaf
     * bumps the leaf key and, for an {@link Operation}, which stamps no leaf keys, this version too.
     */
    int version();

    /**
     * The IR nodes directly under this node, in a stable order.
     */
    @JsonIgnore
    List<? extends IrNode> children();

    /**
     * The version-map entries this node itself stamps, excluding its children's. See {@link IrVersions}.
     */
    @JsonIgnore
    Map<String, Integer> versionEntries();
}
