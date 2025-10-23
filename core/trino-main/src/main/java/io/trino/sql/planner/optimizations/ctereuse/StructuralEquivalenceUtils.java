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

import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class StructuralEquivalenceUtils
{
    private StructuralEquivalenceUtils()
    {}

    public static boolean blocksStructurallyEquivalent(List<Block> leftBlocks, List<Block> rightBlocks)
    {
        return blocksStructurallyEquivalent(leftBlocks, rightBlocks, new HashMap<>());
    }

    /**
     * Compare blocks structure.
     */
    public static boolean blocksStructurallyEquivalent(Block leftBlock, Block rightBlock)
    {
        return blocksStructurallyEquivalent(leftBlock, rightBlock, new HashMap<>());
    }

    private static boolean blocksStructurallyEquivalent(List<Block> leftBlocks, List<Block> rightBlocks, Map<Value, Value> equivalenceMapping)
    {
        if (leftBlocks.size() != rightBlocks.size()) {
            return false;
        }
        return IntStream.range(0, leftBlocks.size())
                .allMatch(i -> blocksStructurallyEquivalent(leftBlocks.get(i), rightBlocks.get(i), new HashMap<>(equivalenceMapping)));
    }

    private static boolean blocksStructurallyEquivalent(Block leftBlock, Block rightBlock, Map<Value, Value> equivalenceMapping)
    {
        if (!leftBlock.parameters().stream().map(Block.Parameter::type).collect(toImmutableList())
                .equals(rightBlock.parameters().stream().map(Block.Parameter::type).collect(toImmutableList()))) {
            return false;
        }

        for (int i = 0; i < leftBlock.parameters().size(); i++) {
            equivalenceMapping.put(rightBlock.parameters().get(i), leftBlock.parameters().get(i));
        }

        List<Operation> leftOperations = leftBlock.operations();
        List<Operation> rightOperations = rightBlock.operations();

        if (leftOperations.size() != rightOperations.size()) {
            return false;
        }

        for (int i = 0; i < leftOperations.size(); i++) {
            if (operationsStructurallyEquivalent(leftOperations.get(i), rightOperations.get(i), equivalenceMapping)) {
                equivalenceMapping.put(rightOperations.get(i).result(), leftOperations.get(i).result());
            }
            else {
                return false;
            }
        }

        return true;
    }

    private static boolean operationsStructurallyEquivalent(Operation leftOperation, Operation rightOperation, Map<Value, Value> equivalenceMapping)
    {
        // compare everything but the result name
        return leftOperation.dialect().equals(rightOperation.dialect()) &&
                leftOperation.name().equals(rightOperation.name()) &&
                leftOperation.result().type().equals(rightOperation.result().type()) &&
                leftOperation.arguments().equals(mapped(rightOperation.arguments(), equivalenceMapping)) &&
                blocksStructurallyEquivalent(
                        leftOperation.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList()),
                        rightOperation.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList()),
                        equivalenceMapping) &&
                ((TrinoOperation) leftOperation).operationAttributes().equals(((TrinoOperation) rightOperation).operationAttributes());
    }

    private static List<Value> mapped(List<Value> values, Map<Value, Value> mapping)
    {
        return values.stream()
                .map(value -> mapping.getOrDefault(value, value))
                .collect(toImmutableList());
    }
}
