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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapParameters;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapValues;

public class StructuralEquivalenceUtils
{
    private StructuralEquivalenceUtils()
    {}

    public static boolean blocksStructurallyEquivalent(List<Block> leftBlocks, List<Block> rightBlocks)
    {
        if (leftBlocks.size() != rightBlocks.size()) {
            return false;
        }
        return IntStream.range(0, leftBlocks.size())
                .allMatch(i -> blocksStructurallyEquivalent(leftBlocks.get(i), rightBlocks.get(i)));
    }

    /**
     * Compare blocks structure.
     */
    public static boolean blocksStructurallyEquivalent(Block leftBlock, Block rightBlock)
    {
        Block rightBlockRemapped = remapParameters(rightBlock, leftBlock.parameters());
        return operationsStructurallyEquivalent(leftBlock.operations(), rightBlockRemapped.operations());
    }

    private static boolean operationsStructurallyEquivalent(List<Operation> leftOperations, List<Operation> rightOperations)
    {
        if (leftOperations.size() != rightOperations.size()) {
            return false;
        }
        if (leftOperations.isEmpty()) {
            return true;
        }
        if (!operationsStructurallyEquivalent(leftOperations.getFirst(), rightOperations.getFirst())) {
            return false;
        }
        return operationsStructurallyEquivalent(
                leftOperations.subList(1, leftOperations.size()),
                rightOperations.subList(1, rightOperations.size()).stream()
                        .map(operation -> remapValues(operation, ImmutableMap.of(rightOperations.getFirst().result(), leftOperations.getFirst().result())))
                        .collect(toImmutableList()));
    }

    private static boolean operationsStructurallyEquivalent(Operation leftOperation, Operation rightOperation)
    {
        // compare everything but the result name
        return leftOperation.dialect().equals(rightOperation.dialect()) &&
                leftOperation.name().equals(rightOperation.name()) &&
                leftOperation.result().type().equals(rightOperation.result().type()) &&
                leftOperation.arguments().equals(rightOperation.arguments()) &&
                blocksStructurallyEquivalent(
                        leftOperation.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList()),
                        rightOperation.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList())) &&
                leftOperation.attributes().equals(rightOperation.attributes());
    }
}
