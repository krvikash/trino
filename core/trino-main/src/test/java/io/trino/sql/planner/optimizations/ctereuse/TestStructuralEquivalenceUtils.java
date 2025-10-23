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
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.newir.Block;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.falsePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.StructuralEquivalenceUtils.blocksStructurallyEquivalent;
import static org.assertj.core.api.Assertions.assertThat;

class TestStructuralEquivalenceUtils
{
    @Test
    public void testSimpleBlocks()
    {
        // compare simple blocks
        assertThat(blocksStructurallyEquivalent(
                truePredicate(Optional.of("^true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()),
                truePredicate(Optional.of("^another_true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator())))
                .isTrue();

        // compare lists of blocks
        assertThat(blocksStructurallyEquivalent(
                ImmutableList.of(
                        truePredicate(Optional.of("^true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()),
                        falsePredicate(Optional.of("^false_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator())),
                ImmutableList.of(
                        truePredicate(Optional.of("^another_true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()),
                        falsePredicate(Optional.of("^another_false_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()))))
                .isTrue();

        // compare different size lists of blocks
        assertThat(blocksStructurallyEquivalent(
                ImmutableList.of(
                        truePredicate(Optional.of("^true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()),
                        falsePredicate(Optional.of("^false_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator())),
                ImmutableList.of(
                        truePredicate(Optional.of("^another_true_predicate"), ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()))))
                .isFalse();
    }

    @Test
    public void testBlockParameters()
    {
        // compare blocks with equivalent parameters
        assertThat(blocksStructurallyEquivalent(
                truePredicate(Optional.of("^true_predicate"), ImmutableList.of(new Block.Parameter("%parameter", irType(BIGINT))), new ProgramBuilder.ValueNameAllocator()),
                truePredicate(Optional.of("^true_predicate"), ImmutableList.of(new Block.Parameter("%another_parameter", irType(BIGINT))), new ProgramBuilder.ValueNameAllocator())))
                .isTrue();

        // compare blocks with different types of parameters
        assertThat(blocksStructurallyEquivalent(
                truePredicate(Optional.of("^true_predicate"), ImmutableList.of(new Block.Parameter("%parameter", irType(BIGINT))), new ProgramBuilder.ValueNameAllocator()),
                falsePredicate(Optional.of("^false_predicate"), ImmutableList.of(new Block.Parameter("%parameter", irType(VARCHAR))), new ProgramBuilder.ValueNameAllocator())))
                .isFalse();

        // compare blocks with different numbers of parameters
        assertThat(blocksStructurallyEquivalent(
                truePredicate(Optional.of("^true_predicate"), ImmutableList.of(new Block.Parameter("%parameter", irType(BIGINT)), new Block.Parameter("%another_parameter", irType(BIGINT))), new ProgramBuilder.ValueNameAllocator()),
                falsePredicate(Optional.of("^false_predicate"), ImmutableList.of(new Block.Parameter("%parameter", irType(BIGINT))), new ProgramBuilder.ValueNameAllocator())))
                .isFalse();
    }

    @Test
    public void testOperations()
    {
        Block.Parameter firstParameter = new Block.Parameter("%first_parameter", irType(anonymousRow(BIGINT, BOOLEAN)));
        FieldReference firstFieldReference = new FieldReference("%first_field_1", firstParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant firstConstant = new Constant("%first_constant", BOOLEAN, true);
        Row firstRow = new Row("%first_row", ImmutableList.of(firstFieldReference.result(), firstConstant.result()), ImmutableList.of(firstFieldReference.attributes(), firstConstant.attributes()));
        Return firstReturn = new Return("%first_return", firstRow.result(), firstRow.attributes());

        Block firstBlock = new Block(
                Optional.of("^first_block"),
                ImmutableList.of(firstParameter),
                ImmutableList.of(firstFieldReference, firstConstant, firstRow, firstReturn));

        Block.Parameter secondParameter = new Block.Parameter("%second_parameter", irType(anonymousRow(BIGINT, BOOLEAN)));
        FieldReference secondFieldReference = new FieldReference("%second_field_1", secondParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant secondConstant = new Constant("%second_constant", BOOLEAN, true);
        Row secondRow = new Row("%second_row", ImmutableList.of(secondFieldReference.result(), secondConstant.result()), ImmutableList.of(secondFieldReference.attributes(), secondConstant.attributes()));
        Return secondReturn = new Return("%second_return", secondRow.result(), secondRow.attributes());

        Block secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondFieldReference, secondConstant, secondRow, secondReturn));

        // compare blocks with equivalent operations
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isTrue();

        // change the order of operations in the second block (semantics remains the same)
        secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondConstant, secondFieldReference, secondRow, secondReturn));
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();

        // change operation attribute in second block: constant boolean is now false
        secondConstant = new Constant("%second_constant", BOOLEAN, false);
        secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondFieldReference, secondConstant, secondRow, secondReturn));
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();

        // change operation returned in second block: constant is now of VARCHAR type
        secondConstant = new Constant("%second_constant", VARCHAR, null);
        secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondFieldReference, secondConstant, secondRow, secondReturn));
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();

        // change operation count in second block: now it has one more operation
        secondConstant = new Constant("%second_constant", BOOLEAN, true);
        Constant additionalConstant = new Constant("%additional_constant", BOOLEAN, true);
        secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondFieldReference, secondConstant, secondRow, additionalConstant, secondReturn));
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();
    }

    @Test
    public void testCorrelatedReference()
    {
        Block.Parameter outerParameter = new Block.Parameter("%outer_parameter", irType(anonymousRow(VARCHAR, BOOLEAN)));

        Block.Parameter firstBlockParameter = new Block.Parameter("%first_block_parameter", irType(anonymousRow(BIGINT)));
        FieldReference firstOuterParameterReference = new FieldReference("%first_outer_parameter_reference", outerParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstRow = new Row(
                "%first_row",
                ImmutableList.of(firstOuterParameterReference.result()),
                ImmutableList.of(firstOuterParameterReference.attributes()));
        Return firstReturn = new Return("%first_return", firstRow.result(), firstRow.attributes());

        Block firstBlock = new Block(
                Optional.of("^block"),
                ImmutableList.of(firstBlockParameter),
                ImmutableList.of(firstOuterParameterReference, firstRow, firstReturn));

        Block.Parameter secondBlockParameter = new Block.Parameter("%second_block_parameter", irType(anonymousRow(BIGINT)));
        FieldReference secondOuterParameterReference = new FieldReference("%second_outer_parameter_reference", outerParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondRow = new Row(
                "%second_row",
                ImmutableList.of(secondOuterParameterReference.result()),
                ImmutableList.of(secondOuterParameterReference.attributes()));
        Return secondReturn = new Return("%second_return", secondRow.result(), secondRow.attributes());

        Block secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondBlockParameter),
                ImmutableList.of(secondOuterParameterReference, secondRow, secondReturn));

        // compare blocks with the same correlated reference
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isTrue();

        // compare blocks with different correlated references: change the correlated reference in second block so that it references a different outer parameter
        Block.Parameter anotherOuterParameter = new Block.Parameter("%another_outer_parameter", irType(anonymousRow(VARCHAR, BOOLEAN)));
        secondOuterParameterReference = new FieldReference("%second_outer_parameter_reference", anotherOuterParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        secondRow = new Row(
                "%second_row",
                ImmutableList.of(secondOuterParameterReference.result()),
                ImmutableList.of(secondOuterParameterReference.attributes()));
        secondReturn = new Return("%second_return", secondRow.result(), secondRow.attributes());

        secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondBlockParameter),
                ImmutableList.of(secondOuterParameterReference, secondRow, secondReturn));

        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();
    }

    @Test
    public void testNestedBlocks()
    {
        Block.Parameter outerParameter = new Block.Parameter("%outer_parameter", irType(anonymousRow(BIGINT, VARCHAR, BOOLEAN)));

        Block.Parameter firstBlockParameter = new Block.Parameter("%first_block_parameter", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter firstLambdaParameter = new Block.Parameter("%first_lambda_parameter", irType(anonymousRow(BOOLEAN)));

        FieldReference firstBlockParameterReference = new FieldReference("%first_block_parameter_reference", firstBlockParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference firstLambdaParameterReference = new FieldReference("%first_lambda_parameter_reference", firstLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference firstOuterParameterReference = new FieldReference("%first_outer_parameter_reference", outerParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstNestedRow = new Row(
                "%first_nested_row",
                ImmutableList.of(firstBlockParameterReference.result(), firstLambdaParameterReference.result(), firstOuterParameterReference.result()),
                ImmutableList.of(firstBlockParameterReference.attributes(), firstLambdaParameterReference.attributes(), firstOuterParameterReference.attributes()));
        Return firstNestedReturn = new Return("%first_nested_return", firstNestedRow.result(), firstNestedRow.attributes());
        Lambda firstLambda = new Lambda(
                "%first_lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(firstLambdaParameter),
                        ImmutableList.of(
                                firstBlockParameterReference,
                                firstLambdaParameterReference,
                                firstOuterParameterReference,
                                firstNestedRow,
                                firstNestedReturn)));
        Return firstReturn = new Return("%first_return", firstLambda.result(), firstLambda.attributes());

        Block firstBlock = new Block(
                Optional.of("^first_block"),
                ImmutableList.of(firstBlockParameter),
                ImmutableList.of(firstLambda, firstReturn));

        Block.Parameter secondBlockParameter = new Block.Parameter("%second_block_parameter", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter secondLambdaParameter = new Block.Parameter("%second_lambda_parameter", irType(anonymousRow(BOOLEAN)));

        FieldReference secondBlockParameterReference = new FieldReference("%second_block_parameter_reference", secondBlockParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondLambdaParameterReference = new FieldReference("%second_lambda_parameter_reference", secondLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondOuterParameterReference = new FieldReference("%second_outer_parameter_reference", outerParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondNestedRow = new Row(
                "%second_nested_row",
                ImmutableList.of(secondBlockParameterReference.result(), secondLambdaParameterReference.result(), secondOuterParameterReference.result()),
                ImmutableList.of(secondBlockParameterReference.attributes(), secondLambdaParameterReference.attributes(), secondOuterParameterReference.attributes()));
        Return secondNestedReturn = new Return("%second_nested_return", secondNestedRow.result(), secondNestedRow.attributes());
        Lambda secondLambda = new Lambda(
                "%second_lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(secondLambdaParameter),
                        ImmutableList.of(
                                secondBlockParameterReference,
                                secondLambdaParameterReference,
                                secondOuterParameterReference,
                                secondNestedRow,
                                secondNestedReturn)));
        Return secondReturn = new Return("%second_return", secondLambda.result(), secondLambda.attributes());

        Block secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondBlockParameter),
                ImmutableList.of(secondLambda, secondReturn));

        // compare blocks with nested operations
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isTrue();
    }

    @Test
    public void testValueNameClashes()
    {
        Block.Parameter firstParameter = new Block.Parameter("%0", irType(anonymousRow(BIGINT, BOOLEAN)));
        FieldReference firstFieldReference = new FieldReference("%1", firstParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference anotherFirstFieldReference = new FieldReference("%2", firstParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant firstConstant = new Constant("%3", BOOLEAN, true);
        Row firstRow = new Row("%4", ImmutableList.of(firstFieldReference.result(), anotherFirstFieldReference.result(), firstConstant.result()), ImmutableList.of(firstFieldReference.attributes(), anotherFirstFieldReference.attributes(), firstConstant.attributes()));
        Return firstReturn = new Return("%5", firstRow.result(), firstRow.attributes());

        Block firstBlock = new Block(
                Optional.of("^first_block"),
                ImmutableList.of(firstParameter),
                ImmutableList.of(firstFieldReference, anotherFirstFieldReference, firstConstant, firstRow, firstReturn));

        // compare identical blocks with the same value names
        assertThat(blocksStructurallyEquivalent(firstBlock, firstBlock)).isTrue();

        Block.Parameter secondParameter = new Block.Parameter("%1", irType(anonymousRow(BIGINT, BOOLEAN)));
        FieldReference secondFieldReference = new FieldReference("%0", secondParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference anotherSecondFieldReference = new FieldReference("%3", secondParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant secondConstant = new Constant("%2", BOOLEAN, true);
        Row secondRow = new Row("%5", ImmutableList.of(secondFieldReference.result(), anotherSecondFieldReference.result(), secondConstant.result()), ImmutableList.of(secondFieldReference.attributes(), anotherSecondFieldReference.attributes(), secondConstant.attributes()));
        Return secondReturn = new Return("%4", secondRow.result(), secondRow.attributes());

        Block secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondFieldReference, anotherSecondFieldReference, secondConstant, secondRow, secondReturn));

        // compare identical blocks with clashing value names
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isTrue();
    }
}
