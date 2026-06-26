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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.clearspring.analytics.util.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata.FIELD_INDEX;
import static io.trino.sql.planner.optimizations.ctereuse.FieldMapping.EMPTY;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.layoutOperations;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.reallocateValues;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapParameters;
import static java.util.HashMap.newHashMap;
import static java.util.function.Function.identity;

public class AssignmentsUtils
{
    private AssignmentsUtils() {}

    // TODO use this method to validate selector blocks in operation constructors
    public static boolean isFieldSelector(Block block)
    {
        return isFieldSelector(block, false);
    }

    public static boolean isFieldSelector(Block block, boolean allowDuplicates)
    {
        if (block.parameters().stream()
                .anyMatch(parameter -> !IS_RELATION_ROW.test(trinoType(parameter.type())))) {
            return false;
        }

        if (!IS_RELATION_ROW.test(trinoType(block.getReturnedType()))) {
            return false;
        }

        if (trinoType(block.getReturnedType()).equals(EMPTY_ROW)) {
            return isEmptyFieldSelector(block);
        }

        if (!(block.operations().size() > 2 &&
                block.operations().getLast() instanceof Return returnOperation &&
                block.operations().get(block.operations().size() - 2) instanceof Row rowConstructor &&
                returnOperation.argument().equals(rowConstructor.result()) &&
                // we require that output fields result from distinct FieldReference operations
                rowConstructor.arguments().stream().distinct().count() == rowConstructor.arguments().size() &&
                block.operations().subList(0, block.operations().size() - 2).stream()
                        .allMatch(FieldReference.class::isInstance))) {
            return false;
        }

        Set<Value> fieldReferences = block.operations().subList(0, block.operations().size() - 2).stream()
                .map(Operation::result)
                .collect(toImmutableSet());
        Set<Value> outputFields = ImmutableSet.copyOf(rowConstructor.arguments());
        if (!fieldReferences.equals(outputFields)) {
            return false;
        }

        Multimap<Value, Integer> referencedFieldsPerRow = LinkedListMultimap.create();
        block.operations().subList(0, block.operations().size() - 2).stream()
                .map(FieldReference.class::cast)
                .forEach(fieldReference -> referencedFieldsPerRow.put(fieldReference.base(), FIELD_INDEX.getAttribute(fieldReference.attributes())));

        if (!allowDuplicates) {
            // we require that each input field is referenced at most once
            if (referencedFieldsPerRow.asMap().values().stream()
                    .anyMatch(fieldIndexes -> fieldIndexes.stream().distinct().count() != fieldIndexes.size())) {
                return false;
            }
        }

        Set<Value> referencedRows = referencedFieldsPerRow.keySet();
        Set<Value> parameters = ImmutableSet.copyOf(block.parameters());
        return parameters.containsAll(referencedRows);
    }

    public static boolean isEmptyFieldSelector(Block block)
    {
        if (block.parameters().stream()
                .anyMatch(parameter -> !IS_RELATION_ROW.test(trinoType(parameter.type())))) {
            return false;
        }

        return trinoType(block.getReturnedType()).equals(EMPTY_ROW) &&
                // empty field selector returns constant null of EmptyRowType
                block.operations().size() == 2 &&
                block.operations().get(0) instanceof Constant constantOperation &&
                block.operations().get(1) instanceof Return returnOperation &&
                returnOperation.argument().equals(constantOperation.result()) &&
                CONSTANT_VALUE.getAttribute(constantOperation.attributes()).equals(ConstantValue.asNull(EMPTY_ROW));
    }

    public static boolean isEmptyRelationalComputation(Block block)
    {
        if (block.parameters().stream()
                .anyMatch(parameter -> !IS_RELATION.test(trinoType(parameter.type())))) {
            return false;
        }

        return trinoType(block.getReturnedType()).equals(EMPTY_ROW) &&
                // empty field selector returns constant null of EmptyRowType
                block.operations().size() == 2 &&
                block.operations().get(0) instanceof Constant constantOperation &&
                block.operations().get(1) instanceof Return returnOperation &&
                returnOperation.argument().equals(constantOperation.result()) &&
                CONSTANT_VALUE.getAttribute(constantOperation.attributes()).equals(ConstantValue.asNull(EMPTY_ROW));
    }

    /**
     * Check if the block passes all input fields to output in the original order
     */
    public static boolean isFullPassthroughFieldSelector(Block block)
    {
        if (!isPruningAssignments(block)) {
            return false;
        }

        if (isEmptyFieldSelector(block)) {
            return trinoType(getOnlyElement(block.parameters()).type()).equals(EMPTY_ROW);
        }

        Row rowConstructor = (Row) block.operations().get(block.operations().size() - 2);

        if (rowConstructor.arguments().size() != trinoType(getOnlyElement(block.parameters()).type()).getTypeParameters().size()) {
            return false;
        }

        Map<Value, Integer> referencedFields = block.operations().subList(0, block.operations().size() - 2).stream()
                .map(FieldReference.class::cast)
                .collect(toImmutableMap(
                        FieldReference::result,
                        fieldReference -> FIELD_INDEX.getAttribute(fieldReference.attributes())));

        for (int i = 0; i < rowConstructor.arguments().size(); i++) {
            if (referencedFields.get(rowConstructor.arguments().get(i)) != i) {
                return false;
            }
        }

        return true;
    }

    // TODO use this method in Project operation constructor to validate the ^assignments block
    // NOTE: a Block with dead code is considered valid Project assignments unless it is an empty selector
    public static boolean isProjectAssignments(Block block)
    {
        if (block.parameters().size() != 1 ||
                !IS_RELATION_ROW.test(trinoType(getOnlyElement(block.parameters()).type())) ||
                !IS_RELATION_ROW.test(trinoType(block.getReturnedType()))) {
            return false;
        }

        if (isEmptyFieldSelector(block)) {
            return true;
        }

        return block.operations().size() > 2 &&
                block.operations().getLast() instanceof Return returnOperation &&
                block.operations().get(block.operations().size() - 2) instanceof Row rowConstructor &&
                returnOperation.argument().equals(rowConstructor.result());
    }

    public static boolean isPruningAssignments(Block block)
    {
        return block.parameters().size() == 1 && isFieldSelector(block);
    }

    /**
     * Return mapping from input fields to output fields
     */
    public static FieldMapping getPassthroughMapping(Block block)
    {
        checkArgument(isPruningAssignments(block), "expected pruning assignments");

        if (isEmptyFieldSelector(block)) {
            return EMPTY;
        }

        Row rowConstructor = (Row) block.operations().get(block.operations().size() - 2);
        Map<Value, Integer> referencedFields = block.operations().subList(0, block.operations().size() - 2).stream()
                .map(FieldReference.class::cast)
                .collect(toImmutableMap(
                        FieldReference::result,
                        fieldReference -> FIELD_INDEX.getAttribute(fieldReference.attributes())));

        ImmutableMap.Builder<Integer, Integer> fieldIndexMapping = ImmutableMap.builder();
        for (int i = 0; i < rowConstructor.arguments().size(); i++) {
            fieldIndexMapping.put(referencedFields.get(rowConstructor.arguments().get(i)), i);
        }

        return new FieldMapping(fieldIndexMapping.buildOrThrow());
    }

    /**
     * Return mapping from output fields to input fields.
     * In case when an input field was mapped to multiple output fields, return mapping for all the output fields to the input field.
     */
    public static FieldMapping getInversedMapping(Block block)
    {
        checkArgument(block.parameters().size() == 1 && isFieldSelector(block, true), "expected field selector block with single parameter");

        if (isEmptyFieldSelector(block)) {
            return EMPTY;
        }

        Row rowConstructor = (Row) block.operations().get(block.operations().size() - 2);
        Map<Value, Integer> referencedFields = block.operations().subList(0, block.operations().size() - 2).stream()
                .map(FieldReference.class::cast)
                .collect(toImmutableMap(
                        FieldReference::result,
                        fieldReference -> FIELD_INDEX.getAttribute(fieldReference.attributes())));

        ImmutableMap.Builder<Integer, Integer> inversedMapping = ImmutableMap.builder();
        for (int i = 0; i < rowConstructor.arguments().size(); i++) {
            inversedMapping.put(i, referencedFields.get(rowConstructor.arguments().get(i)));
        }

        return new FieldMapping(inversedMapping.buildOrThrow());
    }

    /**
     * Extract mappings from input fields to output fields, ignoring other projected expressions.
     * In case of an input field being projected multiple times, return the first occurrence.
     * Ignore correlated field references.
     */
    public static FieldMapping getIdentityMappings(Block block)
    {
        checkArgument(isProjectAssignments(block), "expected project assignments");

        if (isEmptyFieldSelector(block)) {
            return EMPTY;
        }

        Map<Value, Operation> operations = block.operations().stream()
                .collect(toImmutableMap(Operation::result, identity()));
        Block.Parameter parameter = getOnlyElement(block.parameters());
        Map<Integer, Integer> fieldIndexMapping = new HashMap<>();
        Row rowConstructor = (Row) block.operations().get(block.operations().size() - 2);

        for (int i = 0; i < rowConstructor.arguments().size(); i++) {
            Value projectedItem = rowConstructor.arguments().get(i);
            Operation operation = operations.get(projectedItem);
            if (operation instanceof FieldReference fieldReference && fieldReference.base().equals(parameter)) {
                fieldIndexMapping.putIfAbsent(FIELD_INDEX.getAttribute(fieldReference.attributes()), i);
            }
        }

        return new FieldMapping(fieldIndexMapping);
    }

    /**
     * Compute indexes of pruned input fields
     */
    public static Set<Integer> getPrunedFields(Block block)
    {
        FieldMapping mapping = getPassthroughMapping(block);

        Set<Integer> inputFields = IntStream.range(0, trinoType(getOnlyElement(block.parameters()).type()).getTypeParameters().size())
                .boxed()
                .collect(toImmutableSet());

        return Sets.difference(inputFields, mapping.keySet());
    }

    public static Block getPruningAssignments(String blockName, Type type, Set<Integer> fieldsToPrune, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");

        Set<Integer> inputIndexes = IntStream.range(0, type.getTypeParameters().size())
                .boxed()
                .collect(toImmutableSet());
        checkState(inputIndexes.containsAll(fieldsToPrune), "specified fields to prune not in input");

        if (fieldsToPrune.containsAll(inputIndexes)) {
            return getEmptyFieldSelector(blockName, type, nameAllocator);
        }

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(type));
        Block.Builder assignments = new Block.Builder(Optional.of(blockName), ImmutableList.of(parameter));

        ImmutableList.Builder<Operation> fieldReferencesBuilder = ImmutableList.builder();
        for (int i = 0; i < type.getTypeParameters().size(); i++) {
            if (!fieldsToPrune.contains(i)) {
                FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, i, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
                assignments.addOperation(fieldReference);
                fieldReferencesBuilder.add(fieldReference);
            }
        }
        List<Operation> fieldReferences = fieldReferencesBuilder.build();
        Row rowConstructor = new Row(
                nameAllocator.newName(),
                fieldReferences.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                fieldReferences.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        assignments.addOperation(rowConstructor);

        // TODO extract and reuse addReturnOperation() from RelationalProgramBuilder
        Return returnOperation = new Return(
                nameAllocator.newName(),
                rowConstructor.result(),
                rowConstructor.attributes());
        assignments.addOperation(returnOperation);

        return assignments.build();
    }

    public static Block getReorderingAssignments(Type type, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");
        checkArgument(fieldMapping.isReordering(type), "expected reordering mapping");
        checkArgument(!fieldMapping.isIdentity(type), "attempt to create reordering projection for identity mapping");

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(type));
        Block.Builder assignments = new Block.Builder(Optional.of("^assignments"), ImmutableList.of(parameter));

        // mapping is reordering and not identity => type is a RowType, and not EMPTY_ROW
        ImmutableList.Builder<Operation> fieldReferencesBuilder = ImmutableList.builder();
        for (int i = 0; i < type.getTypeParameters().size(); i++) {
            FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, fieldMapping.get(i), DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
            assignments.addOperation(fieldReference);
            fieldReferencesBuilder.add(fieldReference);
        }
        List<Operation> fieldReferences = fieldReferencesBuilder.build();
        Row rowConstructor = new Row(
                nameAllocator.newName(),
                fieldReferences.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                fieldReferences.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        assignments.addOperation(rowConstructor);

        // TODO extract and reuse addReturnOperation() from RelationalProgramBuilder
        Return returnOperation = new Return(
                nameAllocator.newName(),
                rowConstructor.result(),
                rowConstructor.attributes());
        assignments.addOperation(returnOperation);

        return assignments.build();
    }

    // TODO use in RelationalProgramBuilder
    public static Block getEmptyFieldSelector(String blockName, Type type, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(type));
        Block.Builder inputSelector = new Block.Builder(Optional.of(blockName), ImmutableList.of(parameter));

        Constant constantOperation = new Constant(nameAllocator.newName(), EMPTY_ROW, null);
        inputSelector.addOperation(constantOperation);
        Return returnOperation = new Return(nameAllocator.newName(), constantOperation.result(), constantOperation.attributes());
        inputSelector.addOperation(returnOperation);

        return inputSelector.build();
    }

    /**
     * Pass all input fields to output in the original order
     */
    public static Block getFullPassthroughFieldSelector(String blockName, Type type, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");

        if (type.equals(EMPTY_ROW)) {
            return getEmptyFieldSelector(blockName, type, nameAllocator);
        }

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(type));
        Block.Builder inputSelector = new Block.Builder(Optional.of(blockName), ImmutableList.of(parameter));

        ImmutableList.Builder<Operation> fieldReferencesBuilder = ImmutableList.builder();
        for (int i = 0; i < type.getTypeParameters().size(); i++) {
            FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, i, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
            inputSelector.addOperation(fieldReference);
            fieldReferencesBuilder.add(fieldReference);
        }
        List<Operation> fieldReferences = fieldReferencesBuilder.build();
        Row rowConstructor = new Row(
                nameAllocator.newName(),
                fieldReferences.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                fieldReferences.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        inputSelector.addOperation(rowConstructor);

        // TODO extract and reuse addReturnOperation() from RelationalProgramBuilder
        Return returnOperation = new Return(
                nameAllocator.newName(),
                rowConstructor.result(),
                rowConstructor.attributes());
        inputSelector.addOperation(returnOperation);

        return inputSelector.build();
    }

    /**
     * Create a field selector block that selects a single field at the given index.
     *
     * @param wrapInRow if true, wraps the field reference in a Row before returning
     */
    public static Block createSingleFieldSelector(Type inputRowType, int fieldIndex, Optional<String> label, boolean wrapInRow, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(IS_RELATION_ROW.test(inputRowType), "expected relation row type");
        checkArgument(fieldIndex >= 0 && fieldIndex < inputRowType.getTypeParameters().size(), "field index out of bounds");

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(inputRowType));
        Block.Builder builder = new Block.Builder(label, ImmutableList.of(parameter));

        FieldReference fieldReference = new FieldReference(
                nameAllocator.newName(),
                parameter,
                fieldIndex,
                DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        builder.addOperation(fieldReference);

        if (wrapInRow) {
            Row row = new Row(
                    nameAllocator.newName(),
                    ImmutableList.of(fieldReference.result()),
                    ImmutableList.of(fieldReference.attributes()));
            builder.addOperation(row);
            builder.addOperation(new Return(nameAllocator.newName(), row.result(), row.attributes()));
        }
        else {
            builder.addOperation(new Return(nameAllocator.newName(), fieldReference.result(), fieldReference.attributes()));
        }

        return builder.build();
    }

    /**
     * Build a field selector block which selects all fields selected by the provided blocks, in the given order.
     * The resulting block has the same name and parameters as the first provided block.
     */
    public static Block concatenateFieldSelectors(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(!blocks.isEmpty(), "empty blocks list");
        checkArgument(blocks.stream().allMatch(block -> isFieldSelector(block, true)), "expected field selector blocks");
        for (int i = 1; i < blocks.size(); i++) {
            checkArgument(blocks.get(i).parameters().size() == blocks.getFirst().parameters().size(), "all blocks must have the same number of parameters");
            for (int j = 0; j < blocks.getFirst().parameters().size(); j++) {
                checkArgument(
                        trinoType(blocks.get(i).parameters().get(j).type()).equals(trinoType(blocks.getFirst().parameters().get(j).type())),
                        "type mismatch");
            }
        }

        if (blocks.size() == 1 || blocks.stream().allMatch(AssignmentsUtils::isEmptyFieldSelector)) {
            return blocks.getFirst();
        }

        List<Block.Parameter> resultParameters = blocks.getFirst().parameters();
        Block.Builder result = new Block.Builder(blocks.getFirst().name(), resultParameters);
        Map<Value, Operation> fieldReferences = new HashMap<>();
        ImmutableList.Builder<Operation> allSelectedFields = ImmutableList.builder();
        blocks.stream()
                .filter(block -> !isEmptyFieldSelector(block))
                // remap operations in the blocks to use the first block's parameters
                .map(block -> RewriteUtils.remapParameters(block, resultParameters))
                // reallocate operation results for safe composition
                .map(block -> RewriteUtils.reallocateValues(block, nameAllocator))
                .map(Block::operations)
                .flatMap(List::stream)
                .forEach(operation -> {
                    if (operation instanceof FieldReference fieldReference) {
                        result.addOperation(fieldReference);
                        fieldReferences.put(fieldReference.result(), fieldReference);
                    }
                    if (operation instanceof Row row) {
                        allSelectedFields.addAll(row.arguments().stream()
                                .map(fieldReferences::get)
                                .collect(toImmutableList()));
                    }
                });
        Row rowConstructor = new Row(
                nameAllocator.newName(),
                allSelectedFields.build().stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                allSelectedFields.build().stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        result.addOperation(rowConstructor);
        Return returnOperation = new Return(
                nameAllocator.newName(),
                rowConstructor.result(),
                rowConstructor.attributes());
        result.addOperation(returnOperation);

        return result.build();
    }

    /**
     * Get a list of selected input field indexes
     * The provided block must have one parameter
     */
    public static List<Integer> getSelectedFields(Block block)
    {
        checkArgument(isFieldSelector(block, true), "expected field selector block");
        checkArgument(block.parameters().size() == 1, "expected block with single parameter");

        if (isEmptyFieldSelector(block)) {
            return ImmutableList.of();
        }

        Map<Value, Integer> fieldReferences = block.operations().subList(0, block.operations().size() - 2).stream()
                .map(FieldReference.class::cast)
                .collect(toImmutableMap(FieldReference::result, fieldReference -> FIELD_INDEX.getAttribute(fieldReference.attributes())));

        return ((Row) block.operations().get(block.operations().size() - 2)).arguments().stream()
                .map(fieldReferences::get)
                .collect(toImmutableList());
    }

    /**
     * Break up a block selecting a row of items into blocks selecting individual items.
     * The resulting blocks have the same name and parameter as the input block.
     * The order of items is respected.
     */
    public static List<Block> getProjectedItems(Block block, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(isProjectAssignments(block), "expected project assignments");

        if (isEmptyFieldSelector(block)) {
            return ImmutableList.of();
        }

        Row rowConstructor = (Row) block.operations().get(block.operations().size() - 2);
        Map<Value, Operation> operations = newHashMap(block.operations().size() + rowConstructor.arguments().size());
        block.operations().stream()
                .forEach(operation -> operations.put(operation.result(), operation));
        ImmutableList.Builder<Block> projectedItems = ImmutableList.builder();
        for (Value argument : rowConstructor.arguments()) {
            Return returnOperation = new Return(nameAllocator.newName(), argument, Attributes.empty()); // TODO pass source attributes
            operations.put(returnOperation.result(), returnOperation);
            Block.Builder builder = new Block.Builder(block.name(), block.parameters());
            layoutOperations(returnOperation.result(), builder, operations);
            projectedItems.add(builder.build());
        }

        return projectedItems.build();
    }

    /**
     * Collect the component items in a Row.
     * The resulting block has the same parameters as the first component block.
     * <p>
     * Note: Local operation results in the component blocks will be re-mapped to new values. Re-mapping does not affect the semantics,
     * but it helps avoid incorrect duplicate values in case when component blocks originate from the same block.
     * <p>
     * Note: any correlated references in the component blocks will be preserved.
     * If the component blocks belong to different contexts, they might potentially contain identical correlated values with different semantics.
     * Those values would clash in the resulting block. It is up to the caller to avoid this kind of issues. It is recommended
     * to only call this method for uncorrelated blocks or for blocks belonging to the same context.
     */
    public static Block composeProjectedItems(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // to create a block, we need a block parameter representing the input. We cannot create a block when the input type is not known
        checkArgument(!blocks.isEmpty(), "cannot combine 0 blocks");

        List<Block.Parameter> resultParameters = blocks.getFirst().parameters();
        Block.Builder result = new Block.Builder(Optional.empty(), resultParameters);
        ImmutableList.Builder<Value> items = ImmutableList.builder();
        for (Block block : blocks) {
            // remap operations in the block to use the first block's parameters
            Block remapped = remapParameters(block, resultParameters);
            // reallocate operation results for safe composition
            Block reallocated = reallocateValues(remapped, nameAllocator);

            for (Operation operation : reallocated.operations()) {
                if (!(operation instanceof Return returnOperation)) {
                    result.addOperation(operation);
                }
                else {
                    items.add(returnOperation.argument());
                }
            }
        }
        Row rowConstructor = new Row(nameAllocator.newName(), items.build(), emptySourceAttributes(items.build().size())); // TODO pass source attributes when we remove ValueMap
        result.addOperation(rowConstructor);
        Return returnOperation = new Return(nameAllocator.newName(), rowConstructor.result(), rowConstructor.attributes());
        result.addOperation(returnOperation);

        return result.build();
    }
}
