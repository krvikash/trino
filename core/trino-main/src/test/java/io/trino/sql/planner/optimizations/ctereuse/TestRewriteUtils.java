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
import io.trino.spi.type.MultisetType;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.planner.optimizations.ctereuse.ComparatorIgnoringDerivedAttributes.blockComparatorIgnoringDerivedAttributes;
import static io.trino.sql.planner.optimizations.ctereuse.ComparatorIgnoringDerivedAttributes.operationComparatorIgnoringDerivedAttributes;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.reallocateValues;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapParameters;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRewriteUtils
{
    private static final Block.Parameter PARAMETER = new Block.Parameter("%parameter", irType(anonymousRow(BIGINT, BOOLEAN, VARCHAR)));
    private static final Block.Parameter ANOTHER_PARAMETER = new Block.Parameter("%parameter", irType(anonymousRow(SMALLINT, DOUBLE)));

    @Test
    public void testRebaseBlockWithoutFieldReferences()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block blockWithoutFieldReferences = new Block(
                Optional.of("^block_without_field_references"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(constantOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        assertThat(rebaseBlock(blockWithoutFieldReferences, anonymousRow(VARCHAR, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.of(new Block(
                        Optional.of("^block_without_field_references"),
                        // new parameter to match the new type
                        ImmutableList.of(new Block.Parameter("%100", irType(anonymousRow(VARCHAR, BIGINT)))),
                        // no field references in the block, so operations remain the same
                        blockWithoutFieldReferences.operations())));
    }

    @Test
    public void testRebaseBlockWithFieldReferences()
    {
        FieldReference fieldReferenceOperation = new FieldReference("%0", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block blockWithFieldReference = new Block(
                Optional.of("^block_with_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(fieldReferenceOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(VARCHAR, BIGINT)));
        // remap field reference so that is uses the new parameter and the remapped field index. The operation result remains the same (%0)
        FieldReference newFieldReferenceOperation = new FieldReference("%0", newParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        assertThat(rebaseBlock(blockWithFieldReference, anonymousRow(VARCHAR, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)).orElseThrow())
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block_with_field_reference"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(newFieldReferenceOperation, returnOperation)));
    }

    @Test
    public void testRebaseBlockWithNestedFieldReference()
    {
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%2", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                        ImmutableList.of(
                                nestedFieldReference,
                                nestedReturn)));
        Return returnOperation = new Return("%3", lambdaOperation.result(), lambdaOperation.attributes());
        Block blockWithNestedFieldReference = new Block(
                Optional.of("^block_with_nested_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(lambdaOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(VARCHAR, BIGINT)));
        // remap nested field reference so that it uses the new parameter and the remapped field index. The operation result remains the same (%1)
        FieldReference newNestedFieldReference = new FieldReference("%1", newParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Lambda newLambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                        ImmutableList.of(
                                newNestedFieldReference,
                                nestedReturn)));
        assertThat(rebaseBlock(blockWithNestedFieldReference, anonymousRow(VARCHAR, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)).orElseThrow())
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block_with_nested_field_reference"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(newLambdaOperation, returnOperation)));
    }

    @Test
    public void testIdentityMapping()
    {
        FieldReference fieldReferenceOperation = new FieldReference("%0", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block blockWithFieldReference = new Block(
                Optional.of("^block_with_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(fieldReferenceOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto the same type, with identity mapping
        // the original block is returned
        assertThat(rebaseBlock(blockWithFieldReference, anonymousRow(BIGINT, BOOLEAN, VARCHAR), new FieldMapping(ImmutableMap.of(0, 0, 1, 1, 2, 2)), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.of(blockWithFieldReference));
    }

    @Test
    public void testNonIdentityMapping()
    {
        Block.Parameter parameter = new Block.Parameter("%parameter", irType(anonymousRow(BIGINT, BOOLEAN, BIGINT)));
        FieldReference fieldReferenceOperation = new FieldReference("%0", parameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block blockWithFieldReference = new Block(
                Optional.of("^block_with_field_reference"),
                ImmutableList.of(parameter),
                ImmutableList.of(fieldReferenceOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, BIGINT) onto the same type, with mapping 2 -> 0, 0 -> 2, 1-> 1
        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(BIGINT, BOOLEAN, BIGINT)));
        // remap field reference so that is uses the new parameter and the remapped field index. The operation result remains the same (%0)
        FieldReference newFieldReferenceOperation = new FieldReference("%0", newParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        assertThat(rebaseBlock(blockWithFieldReference, anonymousRow(BIGINT, BOOLEAN, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 2, 1, 1)), new ProgramBuilder.ValueNameAllocator(100)).orElseThrow())
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block_with_field_reference"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(newFieldReferenceOperation, returnOperation)));
    }

    @Test
    public void testRebaseOntoBroaderType()
    {
        FieldReference fieldReferenceOperation = new FieldReference("%0", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block blockWithFieldReference = new Block(
                Optional.of("^block_with_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(fieldReferenceOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT, SMALLINT, DOUBLE), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(VARCHAR, BIGINT, SMALLINT, DOUBLE)));
        // remap field reference so that is uses the new parameter and the remapped field index. The operation result remains the same (%0)
        FieldReference newFieldReferenceOperation = new FieldReference("%0", newParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        assertThat(rebaseBlock(blockWithFieldReference, anonymousRow(VARCHAR, BIGINT, SMALLINT, DOUBLE), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)).orElseThrow())
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block_with_field_reference"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(newFieldReferenceOperation, returnOperation)));
    }

    @Test
    public void testFailedRebase()
    {
        FieldReference fieldReferenceOperation = new FieldReference("%0", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block blockWithFieldReference = new Block(
                Optional.of("^block_with_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(fieldReferenceOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        // there is no mapping for the field index 1, so the rebase should fail
        assertThat(rebaseBlock(blockWithFieldReference, anonymousRow(VARCHAR, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testFailedRebaseOnNestedLevel()
    {
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%2", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                        ImmutableList.of(
                                nestedFieldReference,
                                nestedReturn)));
        Return returnOperation = new Return("%3", lambdaOperation.result(), lambdaOperation.attributes());
        Block blockWithNestedFieldReference = new Block(
                Optional.of("^block_with_nested_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(lambdaOperation, returnOperation));

        // rebase block from type (BIGINT, BOOLEAN, VARCHAR) onto type (VARCHAR, BIGINT), with mapping 2 -> 0 (the VARCHAR field) and 0 -> 1 (the BIGINT field)
        // there is no mapping for the field index 1, so the rebase should fail when remapping the lambda body
        assertThat(rebaseBlock(blockWithNestedFieldReference, anonymousRow(VARCHAR, BIGINT), new FieldMapping(ImmutableMap.of(2, 0, 0, 1)), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testUnexpectedParameterCount()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block noParametersBlock = new Block(
                Optional.of("^no_parameters_block"),
                ImmutableList.of(),
                ImmutableList.of(constantOperation, returnOperation));

        assertThatThrownBy(() -> rebaseBlock(noParametersBlock, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected single parameter");

        Block multipleParametersBlock = new Block(
                Optional.of("^multiple_parameters_block"),
                ImmutableList.of(PARAMETER, ANOTHER_PARAMETER),
                ImmutableList.of(constantOperation, returnOperation));

        assertThatThrownBy(() -> rebaseBlock(multipleParametersBlock, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected single parameter");
    }

    @Test
    public void testRebaseIndexthParameter()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block twoParametersBlock = new Block(
                Optional.of("^two_parameters_block"),
                ImmutableList.of(PARAMETER, ANOTHER_PARAMETER),
                ImmutableList.of(constantOperation, returnOperation));

        // rebase second parameter from type (SMALLINT, DOUBLE) onto type (DOUBLE), with mapping 1 -> 0
        assertThat(rebaseBlock(twoParametersBlock, 1, anonymousRow(DOUBLE), new FieldMapping(ImmutableMap.of(1, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.of(new Block(
                        Optional.of("^two_parameters_block"),
                        ImmutableList.of(PARAMETER, new Block.Parameter("%100", irType(anonymousRow(DOUBLE)))),
                        twoParametersBlock.operations())));
    }

    @Test
    public void testInvalidParameterIndex()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block twoParametersBlock = new Block(
                Optional.of("^two_parameters_block"),
                ImmutableList.of(PARAMETER, ANOTHER_PARAMETER),
                ImmutableList.of(constantOperation, returnOperation));

        assertThatThrownBy(() -> rebaseBlock(twoParametersBlock, -1, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("parameter index out of bounds");

        assertThatThrownBy(() -> rebaseBlock(twoParametersBlock, 2, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("parameter index out of bounds");
    }

    @Test
    public void validateMappedTypes()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block blockWithoutFieldReferences = new Block(
                Optional.of("^block_without_field_references"),
                ImmutableList.of(PARAMETER), // (BIGINT, BOOLEAN, VARCHAR)
                ImmutableList.of(constantOperation, returnOperation));

        // empty mapping is allowed
        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(BIGINT)));
        assertThat(rebaseBlock(blockWithoutFieldReferences, anonymousRow(BIGINT), FieldMapping.EMPTY, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(Optional.of(new Block(
                        Optional.of("^block_without_field_references"),
                        ImmutableList.of(newParameter),
                        blockWithoutFieldReferences.operations())));

        // invalid mapping: the new type hasn't got a field at index 10
        assertThatThrownBy(() -> rebaseBlock(blockWithoutFieldReferences, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(2, 10)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("invalid mapping");

        // invalid mapped type: mapping BOOLEAN field to BIGINT field
        assertThatThrownBy(() -> rebaseBlock(blockWithoutFieldReferences, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(1, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("invalid mapping");

        // the new parameter type is not a row type
        assertThatThrownBy(() -> rebaseBlock(blockWithoutFieldReferences, BIGINT, new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected relation row type");
        assertThatThrownBy(() -> rebaseBlock(blockWithoutFieldReferences, new MultisetType(anonymousRow(BIGINT)), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected relation row type");

        // the old parameter type is not a row type
        Block blockWithNonRowParameter = new Block(
                Optional.of("^block_with_non_row_parameter"),
                ImmutableList.of(new Block.Parameter("%non_row_parameter", irType(BIGINT))),
                ImmutableList.of(constantOperation, returnOperation));
        assertThatThrownBy(() -> rebaseBlock(blockWithNonRowParameter, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected relation row type");
        assertThatThrownBy(() -> rebaseBlock(blockWithNonRowParameter, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testInvalidParameterReference()
    {
        // block parameters can only be access through the field reference operation
        // other references are considered illegal when rebasing
        // the below block returns the parameter directly, which is not allowed
        Block blockWithInvalidParameterReference = new Block(
                Optional.of("^block_with_invalid_parameter_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(new Return("%1", PARAMETER, Attributes.empty())));

        assertThatThrownBy(() -> rebaseBlock(blockWithInvalidParameterReference, anonymousRow(BIGINT), new FieldMapping(ImmutableMap.of(0, 0)), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("illegal reference to relational parameter. Only field access operations are allowed");
    }

    @Test
    public void testExtractReferencedFields()
    {
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%2", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                        ImmutableList.of(
                                nestedFieldReference,
                                nestedReturn)));
        FieldReference fieldReference = new FieldReference("%3", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%4", lambdaOperation.result(), lambdaOperation.attributes());

        // references field 1 at the top level, and field 2 in the lambda
        Block blockWithNestedFieldReference = new Block(
                Optional.of("^block_with_nested_field_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(lambdaOperation, fieldReference, returnOperation));

        assertThat(extractReferencedFields(blockWithNestedFieldReference, PARAMETER))
                .isEqualTo(ImmutableSet.of(2, 1));

        // block parameter is an empty row, so there aer no fields to reference
        Block.Parameter emptyRowParameter = new Block.Parameter("%parameter", irType(EMPTY_ROW));
        Block blockBasedOnEmptyRow = new Block(
                Optional.of("^block_with_nested_field_reference"),
                ImmutableList.of(emptyRowParameter),
                ImmutableList.of(lambdaOperation, fieldReference, returnOperation));

        assertThat(extractReferencedFields(blockBasedOnEmptyRow, emptyRowParameter))
                .isEqualTo(ImmutableSet.of());

        // block parameters can only be access through the field reference operation
        // other references are considered illegal when rebasing
        // the below block returns the parameter directly, which is not allowed
        Block blockWithInvalidParameterReference = new Block(
                Optional.of("^block_with_invalid_parameter_reference"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(new Return("%1", PARAMETER, Attributes.empty())));

        assertThatThrownBy(() -> extractReferencedFields(blockWithInvalidParameterReference, PARAMETER))
                .hasMessage("illegal reference to relational parameter. Only field access operations are allowed");

        // the referenced parameter must be of a row type
        Block.Parameter nonRowParameter = new Block.Parameter("%non_row_parameter", irType(BIGINT));
        Block blockWithNonRowParameter = new Block(
                Optional.of("^block_with_non_row_parameter"),
                ImmutableList.of(nonRowParameter),
                ImmutableList.of(returnOperation));

        assertThatThrownBy(() -> extractReferencedFields(blockWithNonRowParameter, nonRowParameter))
                .hasMessage("expected parameter of relation row type");
    }

    @Test
    public void testRemapParameters()
    {
        Block.Parameter lambdaParameter = new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)));
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference lambdaParameterReference = new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%3", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                nestedFieldReference,
                                lambdaParameterReference,
                                nestedReturn)));
        FieldReference topLevelFieldReference = new FieldReference("%4", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference outerFieldReference = new FieldReference("%5", ANOTHER_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%6", lambdaOperation.result(), lambdaOperation.attributes());

        // this block references two fields from PARAMETER: field 1 at the top level, and field 2 in the lambda
        // additionally, it references field 1 from ANOTHER_PARAMETER (outer reference), and field 0 from the lambda parameter
        Block block = new Block(
                Optional.of("^block"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(lambdaOperation, topLevelFieldReference, outerFieldReference, returnOperation));

        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(BIGINT, BOOLEAN, VARCHAR)));
        // when remapping block parameters, only references to the block parameters are remapped. References to the lambda parameters as well as correlated references are not remapped
        FieldReference newNestedFieldReference = new FieldReference("%1", newParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference newTopLevelFieldReference = new FieldReference("%4", newParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        assertThat(remapParameters(block, ImmutableList.of(newParameter)))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(
                                new Lambda(
                                        "%0",
                                        new Block(
                                                Optional.of("^lambda"),
                                                ImmutableList.of(lambdaParameter),
                                                ImmutableList.of(
                                                        // remapped
                                                        newNestedFieldReference,
                                                        // unchanged
                                                        lambdaParameterReference,
                                                        nestedReturn))),
                                // remapped
                                newTopLevelFieldReference,
                                // unchanged
                                outerFieldReference,
                                returnOperation)));
    }

    @Test
    public void testFailedRemapParameters()
    {
        Constant constantOperation = new Constant("%0", BIGINT, 5L);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block twoParametersBlock = new Block(
                Optional.of("^two_parameters_block"),
                ImmutableList.of(PARAMETER, ANOTHER_PARAMETER),
                ImmutableList.of(constantOperation, returnOperation));

        Block.Parameter newParameter = new Block.Parameter("%100", irType(anonymousRow(BIGINT, BOOLEAN, VARCHAR)));
        Block.Parameter newAnotherParameter = new Block.Parameter("%101", irType(anonymousRow(SMALLINT, DOUBLE)));

        // too few parameters
        assertThatThrownBy(() -> remapParameters(twoParametersBlock, ImmutableList.of(newParameter)))
                .hasMessage("type mismatch");

        // too many parameters
        assertThatThrownBy(() -> remapParameters(twoParametersBlock, ImmutableList.of(newParameter, newAnotherParameter, newAnotherParameter)))
                .hasMessage("type mismatch");

        // mismatching parameter types
        assertThatThrownBy(() -> remapParameters(twoParametersBlock, ImmutableList.of(newAnotherParameter, newParameter)))
                .hasMessage("type mismatch");
    }

    @Test
    public void testRemapNonRelationalParameters()
    {
        Block.Parameter nonRelationalParameter = new Block.Parameter("%parameter", irType(BIGINT));
        Block block = new Block(
                Optional.of("^block"),
                ImmutableList.of(nonRelationalParameter),
                ImmutableList.of(new Return("%0", nonRelationalParameter, Attributes.empty())));

        Block.Parameter newParameter = new Block.Parameter("%100", irType(BIGINT));
        assertThat(remapParameters(block, ImmutableList.of(newParameter)))
                .isEqualTo(new Block(
                        Optional.of("^block"),
                        ImmutableList.of(newParameter),
                        ImmutableList.of(new Return("%0", newParameter, Attributes.empty()))));
    }

    @Test
    public void testReallocateValues()
    {
        FieldReference fieldReference = new FieldReference("%0", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference outerFieldReference = new FieldReference("%1", ANOTHER_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%2", ImmutableList.of(fieldReference.result(), outerFieldReference.result()), ImmutableList.of(fieldReference.attributes(), outerFieldReference.attributes()));
        Return returnOperation = new Return("%3", rowOperation.result(), rowOperation.attributes());
        Block block = new Block(
                Optional.of("^block"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(fieldReference, outerFieldReference, rowOperation, returnOperation));

        FieldReference reallocatedFieldReference = new FieldReference("%100", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference reallocatedOuterFieldReference = new FieldReference("%101", ANOTHER_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row reallocatedRowOperation = new Row("%102", ImmutableList.of(reallocatedFieldReference.result(), reallocatedOuterFieldReference.result()), ImmutableList.of(reallocatedFieldReference.attributes(), reallocatedOuterFieldReference.attributes()));
        Return reallocatedReturnOperation = new Return("%103", reallocatedRowOperation.result(), reallocatedRowOperation.attributes());
        assertThat(reallocateValues(block, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block"),
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(reallocatedFieldReference, reallocatedOuterFieldReference, reallocatedRowOperation, reallocatedReturnOperation)));
    }

    @Test
    public void testReallocateNestedValues()
    {
        Block.Parameter lambdaParameter = new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)));
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference lambdaParameterReference = new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%3", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                nestedFieldReference,
                                lambdaParameterReference,
                                nestedReturn)));
        FieldReference topLevelFieldReference = new FieldReference("%4", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference outerFieldReference = new FieldReference("%5", ANOTHER_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%6",
                ImmutableList.of(topLevelFieldReference.result(), outerFieldReference.result()),
                ImmutableList.of(topLevelFieldReference.attributes(), outerFieldReference.attributes()));
        Return returnOperation = new Return("%7", rowOperation.result(), rowOperation.attributes());

        Block block = new Block(
                Optional.of("^block"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(lambdaOperation, topLevelFieldReference, outerFieldReference, rowOperation, returnOperation));

        FieldReference reallocatedNestedFieldReference = new FieldReference("%101", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference reallocatedLambdaParameterReference = new FieldReference("%102", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return reallocatedNestedReturn = new Return("%103", reallocatedNestedFieldReference.result(), reallocatedNestedFieldReference.attributes());
        Lambda reallocatedLambdaOperation = new Lambda(
                "%100",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                reallocatedNestedFieldReference,
                                reallocatedLambdaParameterReference,
                                reallocatedNestedReturn)));
        FieldReference reallocatedTopLevelFieldReference = new FieldReference("%104", PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference reallocatedOuterFieldReference = new FieldReference("%105", ANOTHER_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row reallocatedRowOperation = new Row(
                "%106",
                ImmutableList.of(reallocatedTopLevelFieldReference.result(), reallocatedOuterFieldReference.result()),
                ImmutableList.of(reallocatedTopLevelFieldReference.attributes(), reallocatedOuterFieldReference.attributes()));
        Return reallocatedReturnOperation = new Return("%107", reallocatedRowOperation.result(), reallocatedRowOperation.attributes());

        assertThat(reallocateValues(block, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.of("^block"),
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(reallocatedLambdaOperation, reallocatedTopLevelFieldReference, reallocatedOuterFieldReference, reallocatedRowOperation, reallocatedReturnOperation)));
    }

    @Test
    public void testRemapValues()
    {
        Value oldResult = new Operation.Result("%old_result", irType(BIGINT));
        Value newResult = new Operation.Result("%new_result", irType(BIGINT));
        Value oldParameter = new Block.Parameter("%old_parameter", irType(BOOLEAN));
        Value newParameter = new Block.Parameter("%new_parameter", irType(BOOLEAN));
        Value anotherOldValue = new Operation.Result("%another_old_value", irType(VARCHAR));
        Value anotherNewValue = new Operation.Result("%another_new_value", irType(VARCHAR));

        Row rowOperation = new Row(
                "%row",
                ImmutableList.of(oldResult, oldParameter),
                ImmutableList.of(Attributes.empty(), Attributes.empty()));

        // remap both arguments
        assertThat(remapValues(rowOperation, ImmutableMap.of(oldResult, newResult, oldParameter, newParameter)))
                .isEqualTo(new Row(
                        "%row",
                        ImmutableList.of(newResult, newParameter),
                        ImmutableList.of(Attributes.empty(), Attributes.empty())));

        // remap one argument
        assertThat(remapValues(rowOperation, ImmutableMap.of(oldParameter, newParameter)))
                .isEqualTo(new Row(
                        "%row",
                        ImmutableList.of(oldResult, newParameter),
                        ImmutableList.of(Attributes.empty(), Attributes.empty())));

        // remap nothing
        assertThat(remapValues(rowOperation, ImmutableMap.of(anotherOldValue, anotherNewValue)))
                .isEqualTo(rowOperation);

        // type mismatch: cannot replace a BIGINT with a VARCHAR
        assertThatThrownBy(() -> remapValues(rowOperation, ImmutableMap.of(oldResult, anotherNewValue)))
                .hasMessage("type mismatch");
    }

    @Test
    public void testRemapNestedValues()
    {
        FieldReference nestedFieldReference = new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return nestedReturn = new Return("%2", nestedFieldReference.result(), nestedFieldReference.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                        ImmutableList.of(
                                nestedFieldReference,
                                nestedReturn)));

        // replace the argument to the nested Return operation. It now returns the given value instead of the nested field reference
        Value newResult = new Operation.Result("%new_result", irType(VARCHAR));

        assertThat(remapValues(lambdaOperation, ImmutableMap.of(nestedFieldReference.result(), newResult)))
                .usingComparator(operationComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Lambda(
                        "%0",
                        new Block(
                                Optional.of("^lambda"),
                                ImmutableList.of(new Block.Parameter("%lambda_parameter", irType(anonymousRow(BOOLEAN)))),
                                ImmutableList.of(
                                        // the nested field reference remains as-is, but it becomes dead code (not referenced in the Return operation)
                                        new FieldReference("%1", PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                        // the Return operation now returns the new result. Note that the attributes of the Return operation are preserved
                                        new Return("%2", newResult, nestedFieldReference.attributes())))));
    }
}
