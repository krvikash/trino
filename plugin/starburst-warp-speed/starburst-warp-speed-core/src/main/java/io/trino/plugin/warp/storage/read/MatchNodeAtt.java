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
package io.trino.plugin.warp.storage.read;

import io.trino.plugin.warp.gen.constants.MatchNodeType;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class MatchNodeAtt
{
    static final StructLayout MATCH_NODE_ATT_LAYOUT;
    private static final long MATCH_NODE_ATT_NODE_TYPE;
    private static final long MATCH_NODE_ATT_NUM_CHILDREN;
    private static final long MATCH_NODE_ATT_SUBTREE_SIZE;
    private static final long MATCH_NODE_ATT_LEAF_IX;

    private final MemorySegment matchNodeAtt;

    static {
        MATCH_NODE_ATT_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_BYTE.withName("node_type"),
                ValueLayout.JAVA_BYTE.withName("nchildren"),
                ValueLayout.JAVA_BYTE.withName("subtree_size"),
                ValueLayout.JAVA_BYTE.withName("leaf_ix")).withName("match_tree_node_t");
        MATCH_NODE_ATT_NODE_TYPE = MATCH_NODE_ATT_LAYOUT.byteOffset(PathElement.groupElement("node_type"));
        MATCH_NODE_ATT_NUM_CHILDREN = MATCH_NODE_ATT_LAYOUT.byteOffset(PathElement.groupElement("nchildren"));
        MATCH_NODE_ATT_SUBTREE_SIZE = MATCH_NODE_ATT_LAYOUT.byteOffset(PathElement.groupElement("subtree_size"));
        MATCH_NODE_ATT_LEAF_IX = MATCH_NODE_ATT_LAYOUT.byteOffset(PathElement.groupElement("leaf_ix"));
    }

    public MatchNodeAtt(MemorySegment matchNodeAtt, MatchNodeType nodeType, int leafIx)
    {
        this.matchNodeAtt = matchNodeAtt;
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NODE_TYPE, (byte) nodeType.ordinal());
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NUM_CHILDREN, (byte) 0);
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_SUBTREE_SIZE, (byte) 1);
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_LEAF_IX, (byte) leafIx);
    }

    public MatchNodeAtt(MemorySegment matchNodeAtt, MatchNodeType nodeType, int numChildren, int subTreeSize)
    {
        this.matchNodeAtt = matchNodeAtt;
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NODE_TYPE, (byte) nodeType.ordinal());
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NUM_CHILDREN, (byte) numChildren);
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_SUBTREE_SIZE, (byte) subTreeSize);
        matchNodeAtt.set(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_LEAF_IX, (byte) 0);
    }

    @Override
    public String toString()
    {
        return "MatchNodeAtt{" +
                "matchNodeAtt=" + matchNodeAtt +
                ", nodeType=" + matchNodeAtt.get(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NODE_TYPE) +
                ", numChildren=" + matchNodeAtt.get(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_NUM_CHILDREN) +
                ", subtreeSize=" + matchNodeAtt.get(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_SUBTREE_SIZE) +
                ", leafIx=" + matchNodeAtt.get(ValueLayout.JAVA_BYTE, MATCH_NODE_ATT_LEAF_IX) +
                '}';
    }
}
