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
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.OR;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.booleanNullPredicate;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.extractDisjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.falsePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.filterConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.hoistCommonConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.intersectPredicates;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isNull;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.removeConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.StructuralEquivalenceUtils.blocksStructurallyEquivalent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPredicateUtils
{
    private static final Block.Parameter PARAMETER = new Block.Parameter("%predicateParameter", irType(BOOLEAN));
    private static final Block.Parameter ANOTHER_PARAMETER = new Block.Parameter("%anotherParameter", irType(BOOLEAN));
    private static final Block.Parameter OUTER_PARAMETER = new Block.Parameter("%outerParameter", irType(BOOLEAN));
    private static final RowType ROW_TYPE = anonymousRow(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);
    private static final Block.Parameter ROW_PARAMETER = new Block.Parameter("%rowParameter", irType(ROW_TYPE));
    private static final Block TRUE_ON_ROW = getConstantBooleanPredicateOnRow(true, "^true_predicate_on_row");
    private static final Block FALSE_ON_ROW = getConstantBooleanPredicateOnRow(false, "^false_predicate_on_row");
    private static final Block NULL_ON_ROW = getConstantBooleanPredicateOnRow(null, "^null_predicate_on_row");

    private static final Block TRUE = getConstantBooleanPredicate(true, "^true_predicate");
    private static final Block FALSE = getConstantBooleanPredicate(false, "^false_predicate");
    private static final Block NULL = getConstantBooleanPredicate(null, "^null_predicate");
    private static final Block OUTER_REFERENCE_PREDICATE = getOuterReferencePredicate();
    private static final Block CONJUNCTION = getConjunctionPredicate();
    private static final Block DISJUNCTION = getDisjunctionPredicate();
    private static final Block NOT_A_PREDICATE = getNotAPredicate();

    @Test
    public void testTruePredicate()
    {
        assertThat(truePredicate(Optional.of("^true_predicate"), ImmutableList.of(PARAMETER), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(TRUE);
    }

    @Test
    public void testFalsePredicate()
    {
        assertThat(falsePredicate(Optional.of("^false_predicate"), ImmutableList.of(PARAMETER), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(FALSE);
    }

    @Test
    public void testBooleanNullPredicate()
    {
        assertThat(booleanNullPredicate(Optional.of("^null_predicate"), ImmutableList.of(PARAMETER), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(NULL);
    }

    @Test
    public void testIsTrue()
    {
        assertThat(isTrue(TRUE)).isTrue();
        assertThat(isTrue(FALSE)).isFalse();
        assertThat(isTrue(NULL)).isFalse();
        assertThat(isTrue(OUTER_REFERENCE_PREDICATE)).isFalse();
        assertThat(isTrue(CONJUNCTION)).isFalse();

        // DISJUNCTION can be optimized to TRUE. isTrue() method does not optimize the input predicate, but checks it structurally.
        assertThat(isTrue(DISJUNCTION)).isFalse();

        assertThat(isTrue(TRUE_ON_ROW)).isTrue();
        assertThat(isTrue(NULL_ON_ROW)).isFalse();
        assertThat(isTrue(FALSE_ON_ROW)).isFalse();

        assertThatThrownBy(() -> isTrue(NOT_A_PREDICATE))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testIsNull()
    {
        assertThat(isNull(NULL)).isTrue();
        assertThat(isNull(TRUE)).isFalse();
        assertThat(isNull(FALSE)).isFalse();
        assertThat(isNull(OUTER_REFERENCE_PREDICATE)).isFalse();
        assertThat(isNull(CONJUNCTION)).isFalse();
        assertThat(isNull(DISJUNCTION)).isFalse();

        assertThat(isNull(TRUE_ON_ROW)).isFalse();
        assertThat(isNull(NULL_ON_ROW)).isTrue();

        assertThatThrownBy(() -> isNull(NOT_A_PREDICATE))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testConjunction()
    {
        // TRUE AND FALSE
        // the resulting conjunction is not optimized
        // component operation results are reallocated with the given ValueNameAllocator
        Constant constantTrueOperation = new Constant("%100", BOOLEAN, true);
        Constant constantFalseOperation = new Constant("%102", BOOLEAN, false);
        Logical conjunctionOperation = new Logical(
                "%104",
                ImmutableList.of(constantTrueOperation.result(), constantFalseOperation.result()),
                AND,
                ImmutableList.of(constantTrueOperation.attributes(), constantFalseOperation.attributes()));
        Return returnOperation = new Return("%105", conjunctionOperation.result(), conjunctionOperation.attributes());

        assertThat(conjunction(ImmutableList.of(TRUE, FALSE), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        // block name of the first conjunct
                        Optional.of("^true_predicate"),
                        // parameter of the first conjunct
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(constantTrueOperation, constantFalseOperation, conjunctionOperation, returnOperation)));

        // the correlated reference OUTER_PARAMETER is not remapped
        Logical conjunctionOperationWithOuterReference = new Logical(
                "%103",
                ImmutableList.of(constantTrueOperation.result(), OUTER_PARAMETER),
                AND,
                ImmutableList.of(constantTrueOperation.attributes(), ImmutableMap.of()));
        Return returnOperationWithOuterReference = new Return("%104", conjunctionOperationWithOuterReference.result(), conjunctionOperationWithOuterReference.attributes());

        assertThat(conjunction(ImmutableList.of(TRUE, OUTER_REFERENCE_PREDICATE), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        // block name of the first conjunct
                        Optional.of("^true_predicate"),
                        // parameter of the first conjunct
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(constantTrueOperation, conjunctionOperationWithOuterReference, returnOperationWithOuterReference)));

        // 1 conjunct
        // the input conjunct is returned as-is, without reallocating values
        assertThat(conjunction(ImmutableList.of(NULL), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(NULL);

        // 0 conjuncts
        assertThatThrownBy(() -> conjunction(ImmutableList.of(), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("cannot combine 0 blocks");

        // 1 non-predicate conjunct
        assertThatThrownBy(() -> conjunction(ImmutableList.of(TRUE, NOT_A_PREDICATE), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected blocks returning boolean");
    }

    @Test
    public void testDisjunction()
    {
        // TRUE OR FALSE
        // the resulting disjunction is not optimized
        // component operation results are reallocated with the given ValueNameAllocator
        Constant constantTrueOperation = new Constant("%100", BOOLEAN, true);
        Constant constantFalseOperation = new Constant("%102", BOOLEAN, false);
        Logical disjunctionOperation = new Logical(
                "%104",
                ImmutableList.of(constantTrueOperation.result(), constantFalseOperation.result()),
                OR,
                ImmutableList.of(constantTrueOperation.attributes(), constantFalseOperation.attributes()));
        Return returnOperation = new Return("%105", disjunctionOperation.result(), disjunctionOperation.attributes());

        assertThat(disjunction(ImmutableList.of(TRUE, FALSE), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        // block name of the first disjunct
                        Optional.of("^true_predicate"),
                        // parameter of the first disjunct
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(constantTrueOperation, constantFalseOperation, disjunctionOperation, returnOperation)));

        // the correlated reference OUTER_PARAMETER is not remapped
        Logical disjunctionOperationWithOuterReference = new Logical(
                "%103",
                ImmutableList.of(constantTrueOperation.result(), OUTER_PARAMETER),
                OR,
                ImmutableList.of(constantTrueOperation.attributes(), ImmutableMap.of()));
        Return returnOperationWithOuterReference = new Return("%104", disjunctionOperationWithOuterReference.result(), disjunctionOperationWithOuterReference.attributes());

        assertThat(disjunction(ImmutableList.of(TRUE, OUTER_REFERENCE_PREDICATE), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        // block name of the first disjunct
                        Optional.of("^true_predicate"),
                        // parameter of the first disjunct
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(constantTrueOperation, disjunctionOperationWithOuterReference, returnOperationWithOuterReference)));

        // 1 disjunct
        // the input disjunct is returned as-is, without reallocating values
        assertThat(disjunction(ImmutableList.of(NULL), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(NULL);

        // 0 disjuncts
        assertThatThrownBy(() -> disjunction(ImmutableList.of(), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("cannot combine 0 blocks");

        // 1 non-predicate disjunct
        assertThatThrownBy(() -> disjunction(ImmutableList.of(TRUE, NOT_A_PREDICATE), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected blocks returning boolean");
    }

    @Test
    public void testExtractConjuncts()
    {
        // TRUE AND (FALSE AND NULL) --> [TRUE, FALSE AND NULL]
        // the top-level conjuncts are extracted. Nested conjuncts are not flattened.
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);
        Block nestedConjunction = conjunction(ImmutableList.of(TRUE, conjunction(ImmutableList.of(FALSE, NULL), nameAllocator)), nameAllocator);

        Constant constantTrueOperation = new Constant("%106", BOOLEAN, true);
        Return firstReturnOperation = new Return("%114", constantTrueOperation.result(), constantTrueOperation.attributes());
        Constant constantFalseOperation = new Constant("%108", BOOLEAN, false);
        Constant constantNullOperation = new Constant("%109", BOOLEAN, null);
        Logical nestedConjunctionOperation = new Logical(
                "%110",
                ImmutableList.of(constantFalseOperation.result(), constantNullOperation.result()),
                AND,
                ImmutableList.of(constantFalseOperation.attributes(), constantNullOperation.attributes()));
        Return secondReturnOperation = new Return("%115", nestedConjunctionOperation.result(), nestedConjunctionOperation.attributes());

        assertThat(extractConjuncts(nestedConjunction, nameAllocator))
                .isEqualTo(ImmutableList.of(
                        // TRUE
                        new Block(
                                // block name of the first conjunct
                                Optional.of("^true_predicate"),
                                // parameter of the first conjunct
                                ImmutableList.of(PARAMETER),
                                ImmutableList.of(constantTrueOperation, firstReturnOperation)),
                        // FALSE AND NULL
                        new Block(
                                // block name of the first conjunct
                                Optional.of("^true_predicate"),
                                // parameter of the first conjunct
                                ImmutableList.of(PARAMETER),
                                ImmutableList.of(
                                        constantFalseOperation,
                                        constantNullOperation,
                                        nestedConjunctionOperation,
                                        secondReturnOperation))));

        // not a conjunction --> block is returned as-is
        nameAllocator = new ProgramBuilder.ValueNameAllocator(100);
        Block nestedDisjunction = disjunction(ImmutableList.of(TRUE, disjunction(ImmutableList.of(FALSE, NULL), nameAllocator)), nameAllocator);
        assertThat(extractConjuncts(nestedDisjunction, nameAllocator))
                .isEqualTo(ImmutableList.of(nestedDisjunction));

        // not a predicate
        assertThatThrownBy(() -> extractConjuncts(NOT_A_PREDICATE, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testExtractDisjuncts()
    {
        // this test verifies the structure of blocks, and abstracts from the actual value names, parameter names etc.
        // the exact name checks are done in method testExtractConjuncts()
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // TRUE OR (FALSE OR NULL) --> [TRUE, FALSE OR NULL]
        // the top-level disjuncts are extracted. Nested disjuncts are not flattened.
        assertBlocks(
                extractDisjuncts(disjunction(ImmutableList.of(TRUE, disjunction(ImmutableList.of(FALSE, NULL), nameAllocator)), nameAllocator), nameAllocator),
                ImmutableList.of(TRUE, disjunction(ImmutableList.of(FALSE, NULL), nameAllocator)));

        // TRUE AND (FALSE AND NULL) --> [TRUE AND (FALSE AND NULL)]
        // not a disjunction --> block is returned as-is
        Block nestedConjunction = conjunction(ImmutableList.of(TRUE, conjunction(ImmutableList.of(FALSE, NULL), nameAllocator)), nameAllocator);
        assertBlocks(
                extractDisjuncts(nestedConjunction, nameAllocator),
                ImmutableList.of(nestedConjunction));

        // not a predicate
        assertThatThrownBy(() -> extractDisjuncts(NOT_A_PREDICATE, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testRemoveConjuncts()
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // assert correct remapping of the values
        Constant constantFalseOperation = new Constant("%102", BOOLEAN, false);
        Return returnOperation = new Return("%107", constantFalseOperation.result(), constantFalseOperation.attributes());
        assertThat(removeConjuncts(
                conjunction(ImmutableList.of(TRUE, FALSE), nameAllocator),
                TRUE,
                nameAllocator))
                .isEqualTo(new Block(
                        Optional.of("^true_predicate"),
                        ImmutableList.of(PARAMETER),
                        ImmutableList.of(
                                constantFalseOperation,
                                returnOperation)));

        // remove multiple conjuncts: TRUE and NULL
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(TRUE, FALSE, NULL), nameAllocator),
                        conjunction(ImmutableList.of(NULL, TRUE), nameAllocator),
                        nameAllocator),
                FALSE);

        // remove some of the conjuncts (TRUE), as other (FALSE) are not available in the input
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(TRUE, NULL), nameAllocator),
                        conjunction(ImmutableList.of(FALSE, TRUE), nameAllocator),
                        nameAllocator),
                NULL);

        // remove all conjuncts --> return TRUE
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(FALSE, NULL), nameAllocator),
                        conjunction(ImmutableList.of(NULL, FALSE), nameAllocator),
                        nameAllocator),
                TRUE);

        // remove no conjuncts
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(FALSE, NULL), nameAllocator),
                        TRUE,
                        nameAllocator),
                conjunction(ImmutableList.of(FALSE, NULL), nameAllocator));

        // multiple matches
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(FALSE, NULL, FALSE, TRUE, NULL), nameAllocator),
                        NULL,
                        nameAllocator),
                conjunction(ImmutableList.of(FALSE, FALSE, TRUE), nameAllocator));

        // remove complex subexpression
        Block complexSubexpression = disjunction(ImmutableList.of(conjunction(ImmutableList.of(TRUE, NULL), nameAllocator), FALSE), nameAllocator);
        assertBlock(
                removeConjuncts(
                        conjunction(ImmutableList.of(FALSE, complexSubexpression, TRUE), nameAllocator),
                        complexSubexpression,
                        nameAllocator),
                conjunction(ImmutableList.of(FALSE, TRUE), nameAllocator));

        // does not flatten nested conjunctions
        Block initial = conjunction(ImmutableList.of(TRUE, conjunction(ImmutableList.of(FALSE, NULL), nameAllocator)), nameAllocator);
        assertBlock(
                removeConjuncts(
                        initial,
                        FALSE,
                        nameAllocator),
                initial);

        // input is not a predicate
        assertThatThrownBy(() -> removeConjuncts(NOT_A_PREDICATE, TRUE, nameAllocator))
                .hasMessage("expected block returning boolean");

        // conjuncts to remove is not a predicate
        assertThatThrownBy(() -> removeConjuncts(TRUE, NOT_A_PREDICATE, nameAllocator))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testHoistCommonConjuncts()
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // hoist common conjunct
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(conjunctionOfFields(0, 1, 2), conjunctionOfFields(0, 1), conjunctionOfFields(1, 2)), nameAllocator), nameAllocator),
                conjunction(ImmutableList.of(
                                // common conjunct
                                conjunctionOfFields(1),
                                disjunction(ImmutableList.of(
                                                // residual disjuncts
                                                conjunctionOfFields(0, 2),
                                                conjunctionOfFields(0),
                                                conjunctionOfFields(2)),
                                        nameAllocator)),
                        nameAllocator));

        // nothing to hoist
        Block noCommonConjuncts = disjunction(ImmutableList.of(conjunctionOfFields(0, 2), conjunctionOfFields(0, 1), conjunctionOfFields(1, 2)), nameAllocator);
        assertBlock(
                hoistCommonConjuncts(noCommonConjuncts, nameAllocator),
                noCommonConjuncts);

        // nothing to hoist but residuals get optimized - deduplicated
        Block noCommonConjunctsWithDuplicates = disjunction(ImmutableList.of(conjunctionOfFields(0, 2), conjunctionOfFields(1), conjunctionOfFields(1)), nameAllocator);
        assertBlock(
                hoistCommonConjuncts(noCommonConjunctsWithDuplicates, nameAllocator),
                disjunction(ImmutableList.of(conjunctionOfFields(0, 2), conjunctionOfFields(1)), nameAllocator));

        // hoist multiple common conjuncts in varying order
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(conjunctionOfFields(2, 0, 1), conjunctionOfFields(2, 1, 3), conjunctionOfFields(1, 3, 2)), nameAllocator), nameAllocator),
                conjunction(ImmutableList.of(
                                // common conjuncts - flattened
                                conjunctionOfFields(2),
                                conjunctionOfFields(1),
                                disjunction(ImmutableList.of(
                                                // residual disjuncts - deduplicated
                                                conjunctionOfFields(0),
                                                conjunctionOfFields(3)),
                                        nameAllocator)),
                        nameAllocator));

        // one disjunct is fully hoisted - no residuals
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(conjunctionOfFields(0, 1), conjunctionOfFields(0)), nameAllocator), nameAllocator),
                conjunctionOfFields(0));

        // one disjunct provided - optimize and return
        assertBlock(
                hoistCommonConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0, 1), conjunctionOfFields(0, 1)), nameAllocator), nameAllocator),
                conjunctionOfFields(0, 1));

        // optimize logical expressions
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(TRUE, FALSE, NULL), nameAllocator), nameAllocator),
                TRUE);
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(FALSE, NULL), nameAllocator), nameAllocator),
                NULL);

        // flatten nested disjunctions
        assertBlock(
                hoistCommonConjuncts(disjunction(ImmutableList.of(conjunctionOfFields(0, 1), disjunction(ImmutableList.of(conjunctionOfFields(0, 2), conjunctionOfFields(0, 3)), nameAllocator)), nameAllocator), nameAllocator),
                conjunction(ImmutableList.of(
                                conjunctionOfFields(0),
                                disjunction(ImmutableList.of(
                                                conjunctionOfFields(1),
                                                conjunctionOfFields(2),
                                                conjunctionOfFields(3)),
                                        nameAllocator)),
                        nameAllocator));

        // not a predicate
        assertThatThrownBy(() -> hoistCommonConjuncts(NOT_A_PREDICATE, nameAllocator))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testIntersectPredicates()
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // one common conjunct
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunctionOfFields(0, 1, 2), conjunctionOfFields(0, 1), conjunctionOfFields(1, 2)), nameAllocator),
                conjunctionOfFields(1));

        // multiple common conjuncts in varying order
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunctionOfFields(2, 0, 1), conjunctionOfFields(2, 1, 3), conjunctionOfFields(1, 3, 2)), nameAllocator),
                conjunctionOfFields(2, 1));

        // multiple common conjuncts - deduplicated
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunction(ImmutableList.of(NULL, NULL), nameAllocator), conjunction(ImmutableList.of(NULL, TRUE, NULL), nameAllocator)), nameAllocator),
                NULL);

        // no common conjuncts
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunctionOfFields(0, 1), conjunctionOfFields(1, 2), conjunctionOfFields(0, 2)), nameAllocator),
                TRUE_ON_ROW);

        // one block provided - optimize and return
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunction(ImmutableList.of(NULL, NULL), nameAllocator)), nameAllocator),
                NULL);

        // flatten nested conjunctions
        assertBlock(
                intersectPredicates(ImmutableList.of(conjunctionOfFields(0, 1), conjunction(ImmutableList.of(conjunctionOfFields(0, 2), conjunctionOfFields(0, 3)), nameAllocator)), nameAllocator),
                conjunctionOfFields(0));

        // no blocks provided
        assertThatThrownBy(() -> intersectPredicates(ImmutableList.of(), nameAllocator))
                .hasMessage("cannot combine 0 blocks");

        // not a predicate
        assertThatThrownBy(() -> intersectPredicates(ImmutableList.of(NOT_A_PREDICATE), nameAllocator))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testFilterConjuncts()
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // one conjunct passes
        assertBlock(
                filterConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0, 1, 2), TRUE_ON_ROW), nameAllocator), PredicateUtils::isTrue, nameAllocator),
                TRUE_ON_ROW);

        // all conjuncts pass, and they are flattened
        assertBlock(
                filterConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0), conjunctionOfFields(1)), nameAllocator), predicate -> !isTrue(predicate), nameAllocator),
                conjunctionOfFields(0, 1));

        // all conjuncts pass, and they are deduplicated
        assertBlock(
                filterConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0), conjunctionOfFields(0)), nameAllocator), predicate -> !isTrue(predicate), nameAllocator),
                conjunctionOfFields(0));

        // no conjuncts pass
        assertBlock(
                filterConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0, 1), conjunctionOfFields(1, 2)), nameAllocator), PredicateUtils::isTrue, nameAllocator),
                TRUE_ON_ROW);

        // flatten nested conjunctions
        assertBlock(
                filterConjuncts(conjunction(ImmutableList.of(conjunctionOfFields(0), conjunction(ImmutableList.of(NULL_ON_ROW, conjunctionOfFields(1)), nameAllocator)), nameAllocator), PredicateUtils::isNull, nameAllocator),
                NULL_ON_ROW);

        // not a predicate
        assertThatThrownBy(() -> filterConjuncts(NOT_A_PREDICATE, PredicateUtils::isTrue, nameAllocator))
                .hasMessage("expected block returning boolean");
    }

    @Test
    public void testOptimizeLogicalOperations()
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);

        // flatten and deduplicate conjunctions
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(conjunctionOfFields(0, 1, 2), conjunctionOfFields(1, 2, 3)), nameAllocator)),
                conjunctionOfFields(0, 1, 2, 3));

        // flatten conjunctions and remove trivial terms
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(conjunctionOfFields(0), conjunction(ImmutableList.of(conjunctionOfFields(1), TRUE_ON_ROW), nameAllocator)), nameAllocator)),
                conjunctionOfFields(0, 1));

        //flatten conjunctions and constant-fold
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(conjunctionOfFields(0), conjunction(ImmutableList.of(conjunctionOfFields(1), FALSE_ON_ROW), nameAllocator)), nameAllocator)),
                FALSE_ON_ROW);

        // no terms left - empty conjunction evaluates to TRUE
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(TRUE, conjunction(ImmutableList.of(TRUE, conjunction(ImmutableList.of(TRUE, TRUE), nameAllocator)), nameAllocator)), nameAllocator)),
                TRUE);

        // one term left - unwrap it from conjunction
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(TRUE_ON_ROW, conjunction(ImmutableList.of(TRUE_ON_ROW, conjunctionOfFields(1)), nameAllocator)), nameAllocator)),
                conjunctionOfFields(1));

        // flatten and deduplicate disjunctions
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(disjunction(ImmutableList.of(NULL_ON_ROW, disjunction(ImmutableList.of(conjunctionOfFields(1), NULL_ON_ROW), nameAllocator)), nameAllocator)),
                disjunction(ImmutableList.of(NULL_ON_ROW, conjunctionOfFields(1)), nameAllocator));

        // flatten disjunctions and remove trivial terms
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(disjunction(ImmutableList.of(NULL_ON_ROW, disjunction(ImmutableList.of(conjunctionOfFields(1), FALSE_ON_ROW), nameAllocator)), nameAllocator)),
                disjunction(ImmutableList.of(NULL_ON_ROW, conjunctionOfFields(1)), nameAllocator));

        // flatten disjunctions and constant-fold
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(disjunction(ImmutableList.of(NULL_ON_ROW, disjunction(ImmutableList.of(conjunctionOfFields(1), TRUE_ON_ROW), nameAllocator)), nameAllocator)),
                TRUE_ON_ROW);

        // no terms left - empty disjunction evaluates to FALSE
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(disjunction(ImmutableList.of(FALSE, disjunction(ImmutableList.of(FALSE, disjunction(ImmutableList.of(FALSE, FALSE), nameAllocator)), nameAllocator)), nameAllocator)),
                FALSE);

        // one tem left - unwrap it from disjunction
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(disjunction(ImmutableList.of(FALSE_ON_ROW, disjunction(ImmutableList.of(FALSE_ON_ROW, conjunctionOfFields(1)), nameAllocator)), nameAllocator)),
                conjunctionOfFields(1));

        // optimize on nested level - remove trivial term in nested disjunction
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(conjunction(ImmutableList.of(conjunctionOfFields(0), disjunction(ImmutableList.of(conjunctionOfFields(1), conjunctionOfFields(2), FALSE_ON_ROW), nameAllocator)), nameAllocator)),
                conjunction(ImmutableList.of(conjunctionOfFields(0), disjunction(ImmutableList.of(conjunctionOfFields(1), conjunctionOfFields(2)), nameAllocator)), nameAllocator));

        // optimize on multiple levels of nesting
        assertBlock(
                PredicateUtils.optimizeLogicalOperations(
                        disjunction(ImmutableList.of(
                                        conjunctionOfFields(0),
                                        conjunction(ImmutableList.of(
                                                        TRUE_ON_ROW,
                                                        disjunction(ImmutableList.of(
                                                                        conjunctionOfFields(1),
                                                                        TRUE_ON_ROW),
                                                                nameAllocator)),
                                                nameAllocator)),
                                nameAllocator)),
                TRUE_ON_ROW);
    }

    private static Block conjunctionOfFields(int... fields)
    {
        checkArgument(Arrays.stream(fields).allMatch(field -> field >= 0 && field < ROW_TYPE.getTypeParameters().size()));
        checkArgument(fields.length > 0);

        if (fields.length == 1) {
            FieldReference fieldReference = new FieldReference("%field_" + fields[0], ROW_PARAMETER, fields[0], DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
            Return returnOperation = new Return("%return", fieldReference.result(), fieldReference.attributes());
            return new Block(
                    Optional.of("^field_" + fields[0]),
                    ImmutableList.of(ROW_PARAMETER),
                    ImmutableList.of(fieldReference, returnOperation));
        }

        List<Operation> fieldReferences = Arrays.stream(fields)
                .boxed()
                .map(field -> new FieldReference("%field_" + field, ROW_PARAMETER, field, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES))
                .collect(toImmutableList());
        Logical conjunctionOperation = new Logical(
                "%conjunction",
                fieldReferences.stream().map(Operation::result).collect(toImmutableList()),
                AND,
                fieldReferences.stream().map(Operation::attributes).collect(toImmutableList()));
        Return returnOperation = new Return("%return", conjunctionOperation.result(), conjunctionOperation.attributes());

        return new Block(
                Optional.of("^conjunction_of_fields"),
                ImmutableList.of(ROW_PARAMETER),
                ImmutableList.<Operation>builder()
                        .addAll(fieldReferences)
                        .add(conjunctionOperation)
                        .add(returnOperation)
                        .build());
    }

    private static Block getConstantBooleanPredicate(Boolean value, String blockName)
    {
        Constant constantBooleanOperation = new Constant("%0", BOOLEAN, value);
        Return returnOperation = new Return("%1", constantBooleanOperation.result(), constantBooleanOperation.attributes());

        return new Block(
                Optional.of(blockName),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(constantBooleanOperation, returnOperation));
    }

    private static Block getOuterReferencePredicate()
    {
        return new Block(
                Optional.of("^correlated_predicate"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(
                        new Return("%0", OUTER_PARAMETER, ImmutableMap.of())));
    }

    private static Block getConjunctionPredicate()
    {
        Constant constantTrueOperation = new Constant("%0", BOOLEAN, true);
        Logical conjunctionOperation = new Logical(
                "%1",
                ImmutableList.of(PARAMETER, constantTrueOperation.result(), OUTER_PARAMETER),
                AND,
                ImmutableList.of(ImmutableMap.of(), constantTrueOperation.attributes(), ImmutableMap.of()));
        Return returnOperation = new Return("%2", constantTrueOperation.result(), constantTrueOperation.attributes());

        return new Block(
                Optional.of("^conjunction_predicate"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(constantTrueOperation, conjunctionOperation, returnOperation));
    }

    private static Block getDisjunctionPredicate()
    {
        Constant constantTrueOperation = new Constant("%0", BOOLEAN, true);
        Logical disjunctionOperation = new Logical(
                "%1",
                ImmutableList.of(PARAMETER, constantTrueOperation.result(), OUTER_PARAMETER),
                OR,
                ImmutableList.of(ImmutableMap.of(), constantTrueOperation.attributes(), ImmutableMap.of()));
        Return returnOperation = new Return("%2", constantTrueOperation.result(), constantTrueOperation.attributes());

        return new Block(
                Optional.of("^disjunction_predicate"),
                ImmutableList.of(PARAMETER),
                ImmutableList.of(constantTrueOperation, disjunctionOperation, returnOperation));
    }

    private static Block getNotAPredicate()
    {
        Constant constantBigintNullOperation = new Constant("%0", BIGINT, null);
        Return returnOperation = new Return("%1", constantBigintNullOperation.result(), constantBigintNullOperation.attributes());

        return new Block(
                Optional.of("^not_a_predicate"),
                ImmutableList.of(ANOTHER_PARAMETER),
                ImmutableList.of(constantBigintNullOperation, returnOperation));
    }

    private static Block getConstantBooleanPredicateOnRow(Boolean value, String blockName)
    {
        Constant constantBooleanOperation = new Constant("%0", BOOLEAN, value);
        Return returnOperation = new Return("%1", constantBooleanOperation.result(), constantBooleanOperation.attributes());

        return new Block(
                Optional.of(blockName),
                ImmutableList.of(ROW_PARAMETER),
                ImmutableList.of(constantBooleanOperation, returnOperation));
    }

    private static void assertBlocks(List<Block> expected, List<Block> actual)
    {
        assertThat(expected.size())
                .isEqualTo(actual.size());

        for (int i = 0; i < expected.size(); i++) {
            assertBlock(expected.get(i), actual.get(i));
        }
    }

    private static void assertBlock(Block expected, Block actual)
    {
        assertThat(blocksStructurallyEquivalent(expected, actual))
                .isTrue();
    }
}
