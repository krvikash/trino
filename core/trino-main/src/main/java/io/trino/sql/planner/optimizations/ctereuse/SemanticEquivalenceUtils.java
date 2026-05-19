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
package io.trino.sql.planner.optimizations.ctereuse;

import io.trino.sql.newir.Block;

import java.util.List;

import static io.trino.sql.planner.optimizations.ctereuse.StructuralEquivalenceUtils.blocksStructurallyEquivalent;

/**
 * Utility class for checking semantic equivalence of blocks and operations.
 * <p>
 * Semantic equivalence considers the meaning of the operations rather than just their structure.
 * Semantically equivalent blocks produce the same results, and have the same side effects,
 * although they may differ in structure or order of operations.
 * <p>
 * Note: currently this class delegates to StructuralEquivalenceUtils, and compares blocks structurally. It misses some cases of semantic equivalence.
 */
public class SemanticEquivalenceUtils
{
    private SemanticEquivalenceUtils() {}

    public static boolean blocksSemanticallyEquivalent(List<Block> leftBlocks, List<Block> rightBlocks)
    {
        return blocksStructurallyEquivalent(leftBlocks, rightBlocks);
    }

    public static boolean blocksSemanticallyEquivalent(Block leftBlock, Block rightBlock)
    {
        return blocksStructurallyEquivalent(leftBlock, rightBlock);
    }
}
