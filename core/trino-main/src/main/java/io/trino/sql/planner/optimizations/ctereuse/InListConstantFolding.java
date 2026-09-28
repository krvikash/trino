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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue.isOrContainsTypeComparedByValue;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.function.Function.identity;

public final class InListConstantFolding
{
    private InListConstantFolding() {}

    /**
     * Fold constant In value lists in all blocks before CTE reuse processes them.
     */
    public static Program foldConstantInLists(Program program)
    {
        Query query = (Query) program.root();
        return foldNestedInLists(query)
                .map(Program::new)
                .orElse(program);
    }

    private static Optional<Operation> foldNestedInLists(Operation operation)
    {
        // Compatible table groups retain the original TableScan instances by identity.
        if (operation instanceof TableScan) {
            return Optional.empty();
        }

        ImmutableList.Builder<Region> regions = ImmutableList.builderWithExpectedSize(operation.regions().size());
        boolean changed = false;
        for (Region region : operation.regions()) {
            Block block = region.getOnlyBlock();
            Optional<Block> foldedBlock = foldNestedInLists(block);
            regions.add(singleBlockRegion(foldedBlock.orElse(block)));
            changed |= foldedBlock.isPresent();
        }
        return changed
                ? Optional.of(((TrinoOperation) operation).withRegions(regions.build()))
                : Optional.empty();
    }

    private static Optional<Block> foldNestedInLists(Block block)
    {
        // First validate operation result references and fold lists in nested regions, rebuilding
        // only operations whose regions changed.
        // Each block has a separate set. Operation results are identified by name and type within a block.
        Set<Value> availableResults = new HashSet<>();
        Block.Builder builder = new Block.Builder(block.name(), block.parameters());
        boolean changed = false;
        for (Operation operation : block.operations()) {
            // An operation result can only be used after it was declared in the same block.
            // Block parameters are intentionally not checked, as parameters from outer scopes
            // remain visible in nested blocks.
            for (Value argument : operation.arguments()) {
                checkState(
                        !(argument instanceof Operation.Result) || availableResults.contains(argument),
                        "Operation result %s is referenced before its declaration or outside its declaring block",
                        argument.name());
            }

            Optional<Operation> foldedOperation = foldNestedInLists(operation);
            builder.addOperation(foldedOperation.orElse(operation));
            changed |= foldedOperation.isPresent();
            availableResults.add(operation.result());
        }

        Block blockWithFoldedNestedRegions = changed ? builder.build() : block;

        // Record direct consumers and use counts. Result locality verified above guarantees that
        // uses from nested blocks do not need to be included.
        Map<Value, Operation> operations = blockWithFoldedNestedRegions.operations().stream()
                .collect(toImmutableMap(Operation::result, identity()));

        Map<Value, Set<Operation>> consumers = new HashMap<>();
        Map<Value, Integer> remainingUses = new HashMap<>();
        for (Operation operation : blockWithFoldedNestedRegions.operations()) {
            for (Value argument : operation.arguments()) {
                consumers.computeIfAbsent(argument, _ -> newIdentityHashSet())
                        .add(operation);
                remainingUses.merge(argument, 1, Integer::sum);
            }
        }

        // Fold arrays composed entirely of local constants when they are used only as IN value lists.
        Map<Value, Constant> foldedArrays = new HashMap<>();
        Set<Value> constantsToRemove = new HashSet<>();
        for (Operation operation : blockWithFoldedNestedRegions.operations()) {
            if (!(operation instanceof Array array) || !isUsedOnlyAsInValueList(array, consumers)) {
                continue;
            }

            ArrayType arrayType = (ArrayType) trinoType(array.result().type());
            // ConstantValue compares structural values containing these types by Java object identity.
            // Separately folded but semantically equal lists would therefore not compare as equal.
            if (!arrayType.isComparable() || isOrContainsTypeComparedByValue(arrayType)) {
                continue;
            }

            List<Constant> elements = array.arguments().stream()
                    .map(operations::get)
                    .takeWhile(Constant.class::isInstance)
                    .map(Constant.class::cast)
                    .collect(toImmutableList());
            if (elements.size() != array.arguments().size()) {
                continue;
            }

            Type elementType = arrayType.getElementType();
            BlockBuilder valueBuilder = elementType.createBlockBuilder(null, elements.size());
            for (Constant element : elements) {
                ConstantValue constantValue = CONSTANT_VALUE.getAttribute(element.attributes());
                writeNativeValue(elementType, valueBuilder, constantValue.getValue());
            }
            foldedArrays.put(array.result(), new Constant(array.result().name(), arrayType, valueBuilder.build()));

            for (Value argument : array.arguments()) {
                remainingUses.compute(argument, (_, uses) -> uses - 1);
                constantsToRemove.add(argument);
            }
        }

        if (foldedArrays.isEmpty()) {
            return changed ? Optional.of(blockWithFoldedNestedRegions) : Optional.empty();
        }

        // Replace folded arrays and remove element constants that no longer have any uses.
        builder = new Block.Builder(blockWithFoldedNestedRegions.name(), blockWithFoldedNestedRegions.parameters());
        for (Operation operation : blockWithFoldedNestedRegions.operations()) {
            Constant foldedArray = foldedArrays.get(operation.result());
            if (foldedArray != null) {
                builder.addOperation(foldedArray);
                continue;
            }
            if (operation instanceof Constant &&
                    constantsToRemove.contains(operation.result()) &&
                    remainingUses.getOrDefault(operation.result(), 0) == 0) {
                continue;
            }
            builder.addOperation(operation);
        }
        return Optional.of(builder.build());
    }

    private static boolean isUsedOnlyAsInValueList(Array array, Map<Value, Set<Operation>> consumers)
    {
        Set<Operation> arrayConsumers = consumers.getOrDefault(array.result(), Set.of());
        return !arrayConsumers.isEmpty() &&
                arrayConsumers.stream()
                        .allMatch(operation -> operation instanceof In in && in.valueList().equals(array.result()));
    }
}
