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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Comparison;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata.ComparisonOperator.LESS_THAN;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.composeProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.concatenateFieldSelectors;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getIdentityMappings;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getInversedMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getProjectedItems;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPrunedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getReorderingAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyRelationalComputation;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isProjectAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.ComparatorIgnoringDerivedAttributes.blockComparatorIgnoringDerivedAttributes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAssignmentsUtils
{
    private static final Type RELATION_ROW_TYPE = anonymousRow(BIGINT, BOOLEAN, VARCHAR);
    private static final Type RELATION_TYPE = new MultisetType(RELATION_ROW_TYPE);

    private static final Parameter RELATION_ROW_PARAMETER = new Parameter("%relationRowParameter", irType(RELATION_ROW_TYPE));
    private static final Parameter OUTER_RELATION_ROW_PARAMETER = new Parameter("%outerParameter", irType(anonymousRow(BOOLEAN, BOOLEAN, BOOLEAN, SMALLINT)));
    private static final Parameter RELATION_PARAMETER = new Parameter("%relationParameter", irType(RELATION_TYPE));
    private static final Parameter EMPTY_ROW_PARAMETER = new Parameter("%emptyRowParameter", irType(EMPTY_ROW));

    private static final Block FIELD_SELECTOR_WITHOUT_DUPLICATES = getFieldSelectorWithoutDuplicates();
    private static final Block FIELD_SELECTOR_WITH_DUPLICATES = getFieldSelectorWithDuplicates();
    private static final Block FIELD_SELECTOR_WITH_DEAD_CODE = getFieldSelectorWithDeadCode();
    private static final Block FIELD_SELECTOR_WITH_OUTER_REFERENCE = getFieldSelectorWithOuterReference();

    private static final Block FIELD_SELECTOR_FULL_PASSTHROUGH = getFieldSelectorFullPassthrough();
    private static final Block FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES = getFieldSelectorFullPassthroughWithDuplicates();
    private static final Block FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE = getFieldSelectorFullPassthroughWithDeadCode();
    private static final Block FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE = getFieldSelectorFullPassthroughWithOuterReference();
    private static final Block FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX = getFieldSelectorFullPassthroughPrefix();

    private static final Block FIELD_SELECTOR_REORDERING = getFieldSelectorReordering();
    private static final Block FIELD_SELECTOR_REORDERING_WITH_DUPLICATES = getFieldSelectorReorderingWithDuplicates();
    private static final Block FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE = getFieldSelectorReorderingWithDeadCode();
    private static final Block FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE = getFieldSelectorReorderingWithOuterReference();

    private static final Block EMPTY_FIELD_SELECTOR = getEmptyFieldSelectorBlock();
    private static final Block EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE = getEmptyFieldSelectorWithDeadCode();

    private static final Block MALFORMED_FIELD_SELECTOR = getMalformedFieldSelector();
    private static final Block COMPLEX_EXPRESSION_ASSIGNMENTS = getComplexExpressionAssignments();
    private static final Block PRIMITIVE_VALUE_SELECTOR = getPrimitiveValueSelector();
    private static final Block EMPTY_RELATIONAL_COMPUTATION = getEmptyRelationalComputation();
    private static final Block NON_EMPTY_RELATIONAL_COMPUTATION = getNonEmptyRelationalComputation();
    private static final Block PRIMITIVE_INPUT_BLOCK = getPrimitiveInputBlock();
    private static final Block MULTIPLE_PARAMETERS_BLOCK = getMultipleParametersBlock();

    private static final Block EMPTY_COMPUTATION_ON_EMPTY_ROW = getEmptyComputationOnEmptyRow();
    private static final Block NON_EMPTY_COMPUTATION_ON_EMPTY_ROW = getNonEmptyComputationOnEmptyRow();

    @Test
    public void testTypeConstraints()
    {
        assertThat(IS_RELATION_ROW.test(RELATION_ROW_TYPE)).isTrue();
        assertThat(IS_RELATION.test(RELATION_ROW_TYPE)).isFalse();

        assertThat(IS_RELATION_ROW.test(RELATION_TYPE)).isFalse();
        assertThat(IS_RELATION.test(RELATION_TYPE)).isTrue();

        assertThat(IS_RELATION_ROW.test(EMPTY_ROW)).isTrue();
        assertThat(IS_RELATION.test(EMPTY_ROW)).isFalse();

        assertThat(IS_RELATION_ROW.test(BIGINT)).isFalse();
        assertThat(IS_RELATION.test(BIGINT)).isFalse();
    }

    @Test
    public void testIsFieldSelectorDuplicatesNotAllowed()
    {
        assertThat(isFieldSelector(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_DUPLICATES)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFieldSelector(EMPTY_FIELD_SELECTOR)).isTrue();
        assertThat(isFieldSelector(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isFieldSelector(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isFieldSelector(COMPLEX_EXPRESSION_ASSIGNMENTS)).isFalse();
        assertThat(isFieldSelector(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isFieldSelector(EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isFieldSelector(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isFieldSelector(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isFieldSelector(MULTIPLE_PARAMETERS_BLOCK)).isTrue();
        assertThat(isFieldSelector(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
        assertThat(isFieldSelector(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
    }

    @Test
    public void testIsFieldSelectorDuplicatesAllowed()
    {
        assertThat(isFieldSelector(FIELD_SELECTOR_WITHOUT_DUPLICATES, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_DUPLICATES, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_DEAD_CODE, true)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_WITH_OUTER_REFERENCE, true)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE, true)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE, true)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES, true)).isTrue();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE, true)).isFalse();
        assertThat(isFieldSelector(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE, true)).isFalse();
        assertThat(isFieldSelector(EMPTY_FIELD_SELECTOR, true)).isTrue();
        assertThat(isFieldSelector(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE, true)).isFalse();
        assertThat(isFieldSelector(MALFORMED_FIELD_SELECTOR, true)).isFalse();
        assertThat(isFieldSelector(COMPLEX_EXPRESSION_ASSIGNMENTS, true)).isFalse();
        assertThat(isFieldSelector(PRIMITIVE_VALUE_SELECTOR, true)).isFalse();
        assertThat(isFieldSelector(EMPTY_RELATIONAL_COMPUTATION, true)).isFalse();
        assertThat(isFieldSelector(NON_EMPTY_RELATIONAL_COMPUTATION, true)).isFalse();
        assertThat(isFieldSelector(PRIMITIVE_INPUT_BLOCK, true)).isFalse();
        assertThat(isFieldSelector(MULTIPLE_PARAMETERS_BLOCK, true)).isTrue();
        assertThat(isFieldSelector(EMPTY_COMPUTATION_ON_EMPTY_ROW, true)).isTrue();
        assertThat(isFieldSelector(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW, true)).isFalse();
    }

    @Test
    public void testIsEmptyFieldSelector()
    {
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_REORDERING)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyFieldSelector(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyFieldSelector(EMPTY_FIELD_SELECTOR)).isTrue();
        assertThat(isEmptyFieldSelector(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyFieldSelector(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isEmptyFieldSelector(COMPLEX_EXPRESSION_ASSIGNMENTS)).isFalse();
        assertThat(isEmptyFieldSelector(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isEmptyFieldSelector(EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isEmptyFieldSelector(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isEmptyFieldSelector(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isEmptyFieldSelector(MULTIPLE_PARAMETERS_BLOCK)).isFalse();
        assertThat(isEmptyFieldSelector(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
        assertThat(isEmptyFieldSelector(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
    }

    @Test
    public void testIsEmptyRelationalComputation()
    {
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_FULL_PASSTHROUGH)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_REORDERING)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyRelationalComputation(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isEmptyRelationalComputation(EMPTY_FIELD_SELECTOR)).isFalse();
        assertThat(isEmptyRelationalComputation(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isEmptyRelationalComputation(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isEmptyRelationalComputation(COMPLEX_EXPRESSION_ASSIGNMENTS)).isFalse();
        assertThat(isEmptyRelationalComputation(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isEmptyRelationalComputation(EMPTY_RELATIONAL_COMPUTATION)).isTrue();
        assertThat(isEmptyRelationalComputation(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isEmptyRelationalComputation(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isEmptyRelationalComputation(MULTIPLE_PARAMETERS_BLOCK)).isFalse();
        assertThat(isEmptyRelationalComputation(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
        assertThat(isEmptyRelationalComputation(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
    }

    @Test
    public void testIsFullPassthroughFieldSelector()
    {
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_WITH_DUPLICATES)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH)).isTrue();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_REORDERING)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(EMPTY_FIELD_SELECTOR)).isFalse();
        assertThat(isFullPassthroughFieldSelector(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isFullPassthroughFieldSelector(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isFullPassthroughFieldSelector(COMPLEX_EXPRESSION_ASSIGNMENTS)).isFalse();
        assertThat(isFullPassthroughFieldSelector(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isFullPassthroughFieldSelector(EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isFullPassthroughFieldSelector(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isFullPassthroughFieldSelector(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isFullPassthroughFieldSelector(MULTIPLE_PARAMETERS_BLOCK)).isFalse();
        assertThat(isFullPassthroughFieldSelector(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
        assertThat(isFullPassthroughFieldSelector(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
    }

    @Test
    public void testIsProjectAssignments()
    {
        // note: a Block with dead code is considered valid Project assignments unless it is an empty selector
        assertThat(isProjectAssignments(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_WITH_DUPLICATES)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_WITH_DEAD_CODE)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_REORDERING)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isTrue();
        assertThat(isProjectAssignments(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isTrue();
        assertThat(isProjectAssignments(EMPTY_FIELD_SELECTOR)).isTrue();
        assertThat(isProjectAssignments(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isProjectAssignments(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isProjectAssignments(COMPLEX_EXPRESSION_ASSIGNMENTS)).isTrue();
        assertThat(isProjectAssignments(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isProjectAssignments(EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isProjectAssignments(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isProjectAssignments(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isProjectAssignments(MULTIPLE_PARAMETERS_BLOCK)).isFalse();
        assertThat(isProjectAssignments(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
        assertThat(isProjectAssignments(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
    }

    @Test
    public void testIsPruningAssignments()
    {
        assertThat(isPruningAssignments(FIELD_SELECTOR_WITHOUT_DUPLICATES)).isTrue();
        assertThat(isPruningAssignments(FIELD_SELECTOR_WITH_DUPLICATES)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH)).isTrue();
        assertThat(isPruningAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX)).isTrue();
        assertThat(isPruningAssignments(FIELD_SELECTOR_REORDERING)).isTrue();
        assertThat(isPruningAssignments(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE)).isFalse();
        assertThat(isPruningAssignments(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE)).isFalse();
        assertThat(isPruningAssignments(EMPTY_FIELD_SELECTOR)).isTrue();
        assertThat(isPruningAssignments(EMPTY_FIELD_SELECTOR_WITH_DEAD_CODE)).isFalse();
        assertThat(isPruningAssignments(MALFORMED_FIELD_SELECTOR)).isFalse();
        assertThat(isPruningAssignments(COMPLEX_EXPRESSION_ASSIGNMENTS)).isFalse();
        assertThat(isPruningAssignments(PRIMITIVE_VALUE_SELECTOR)).isFalse();
        assertThat(isPruningAssignments(EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isPruningAssignments(NON_EMPTY_RELATIONAL_COMPUTATION)).isFalse();
        assertThat(isPruningAssignments(PRIMITIVE_INPUT_BLOCK)).isFalse();
        assertThat(isPruningAssignments(MULTIPLE_PARAMETERS_BLOCK)).isFalse();
        assertThat(isPruningAssignments(EMPTY_COMPUTATION_ON_EMPTY_ROW)).isTrue();
        assertThat(isPruningAssignments(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW)).isFalse();
    }

    @Test
    public void testGetPassthroughMapping()
    {
        assertThat(getPassthroughMapping(FIELD_SELECTOR_WITHOUT_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        2, 0,
                        0, 1)));

        assertThat(getPassthroughMapping(FIELD_SELECTOR_FULL_PASSTHROUGH))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getPassthroughMapping(FIELD_SELECTOR_REORDERING))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        1, 0,
                        2, 1,
                        0, 2)));

        assertThat(getPassthroughMapping(EMPTY_FIELD_SELECTOR))
                .isEqualTo(FieldMapping.EMPTY);

        assertThat(getPassthroughMapping(EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(FieldMapping.EMPTY);

        assertThatThrownBy(() -> getPassthroughMapping(MULTIPLE_PARAMETERS_BLOCK))
                .hasMessage("expected pruning assignments");
    }

    @Test
    public void testGetInversedMapping()
    {
        assertThat(getInversedMapping(FIELD_SELECTOR_WITHOUT_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0)));

        assertThat(getInversedMapping(FIELD_SELECTOR_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0,
                        2, 2)));

        assertThat(getInversedMapping(FIELD_SELECTOR_FULL_PASSTHROUGH))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getInversedMapping(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2,
                        3, 1)));

        assertThat(getInversedMapping(FIELD_SELECTOR_REORDERING))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 1,
                        1, 2,
                        2, 0)));

        assertThat(getInversedMapping(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 1,
                        1, 2,
                        2, 0,
                        3, 2)));

        assertThat(getInversedMapping(EMPTY_FIELD_SELECTOR))
                .isEqualTo(FieldMapping.EMPTY);

        assertThat(getInversedMapping(EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(FieldMapping.EMPTY);

        assertThatThrownBy(() -> getInversedMapping(MULTIPLE_PARAMETERS_BLOCK))
                .hasMessage("expected field selector block with single parameter");
    }

    @Test
    public void testGetIdentityMappings()
    {
        assertThat(getIdentityMappings(FIELD_SELECTOR_WITHOUT_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        2, 0,
                        0, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        2, 0,
                        0, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_WITH_DEAD_CODE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        2, 0,
                        0, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_WITH_OUTER_REFERENCE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(2, 0)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_FULL_PASSTHROUGH))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DEAD_CODE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_OUTER_REFERENCE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_FULL_PASSTHROUGH_PREFIX))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_REORDERING))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0,
                        2, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0,
                        2, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_REORDERING_WITH_DEAD_CODE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0,
                        2, 1)));

        assertThat(getIdentityMappings(FIELD_SELECTOR_REORDERING_WITH_OUTER_REFERENCE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 2,
                        1, 0,
                        2, 1)));

        assertThat(getIdentityMappings(EMPTY_FIELD_SELECTOR))
                .isEqualTo(FieldMapping.EMPTY);

        assertThat(getIdentityMappings(COMPLEX_EXPRESSION_ASSIGNMENTS))
                .isEqualTo(new FieldMapping(ImmutableMap.of(0, 1)));

        assertThat(getIdentityMappings(EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(FieldMapping.EMPTY);

        assertThat(getIdentityMappings(NON_EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(FieldMapping.EMPTY);

        assertThatThrownBy(() -> getIdentityMappings(MULTIPLE_PARAMETERS_BLOCK))
                .hasMessage("expected project assignments");
    }

    @Test
    public void testGetPrunedFields()
    {
        assertThat(getPrunedFields(FIELD_SELECTOR_WITHOUT_DUPLICATES))
                .isEqualTo(ImmutableSet.of(1));

        assertThat(getPrunedFields(FIELD_SELECTOR_FULL_PASSTHROUGH))
                .isEqualTo(ImmutableSet.of());

        assertThat(getPrunedFields(FIELD_SELECTOR_REORDERING))
                .isEqualTo(ImmutableSet.of());

        assertThat(getPrunedFields(EMPTY_FIELD_SELECTOR))
                .isEqualTo(ImmutableSet.of(0, 1, 2));

        assertThat(getPrunedFields(EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(ImmutableSet.of());

        assertThatThrownBy(() -> getPrunedFields(MULTIPLE_PARAMETERS_BLOCK))
                .hasMessage("expected pruning assignments");
    }

    @Test
    public void testGetPruningAssignments()
    {
        Parameter relationRowParameter = new Parameter("%0", irType(RELATION_ROW_TYPE));

        // prune some fields
        FieldReference field1Reference = new FieldReference("%1", relationRowParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row field1Row = new Row("%2", ImmutableList.of(field1Reference.result()), ImmutableList.of(field1Reference.attributes()));
        Return field1Return = new Return("%3", field1Row.result(), field1Row.attributes());
        assertThat(getPruningAssignments("^result", RELATION_ROW_TYPE, ImmutableSet.of(0, 2), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(field1Reference, field1Row, field1Return)));

        // prune all fields
        Constant emptyRow = new Constant("%1", EMPTY_ROW, null);
        Return emptyReturn = new Return("%2", emptyRow.result(), emptyRow.attributes());
        assertThat(getPruningAssignments("^result", RELATION_ROW_TYPE, ImmutableSet.of(0, 2, 1), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(emptyRow, emptyReturn)));

        // prune no fields
        FieldReference firstFieldReference = new FieldReference("%1", relationRowParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%2", relationRowParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%3", relationRowParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row allFieldsRow = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return allFieldsReturn = new Return("%5", allFieldsRow.result(), allFieldsRow.attributes());
        assertThat(getPruningAssignments("^result", RELATION_ROW_TYPE, ImmutableSet.of(), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, allFieldsRow, allFieldsReturn)));

        // empty input
        Parameter emptyRowParameter = new Parameter("%0", irType(EMPTY_ROW));
        assertThat(getPruningAssignments("^result", EMPTY_ROW, ImmutableSet.of(), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(emptyRowParameter),
                        ImmutableList.of(emptyRow, emptyReturn)));

        // field to prune not in input
        assertThatThrownBy(() -> getPruningAssignments("^result", RELATION_ROW_TYPE, ImmutableSet.of(0, 5, 1), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("specified fields to prune not in input");

        // input type is not a relation row: it is relational
        assertThatThrownBy(() -> getPruningAssignments("^result", RELATION_TYPE, ImmutableSet.of(0, 2, 1), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");

        // input type is not a relation row: it is primitive
        assertThatThrownBy(() -> getPruningAssignments("^result", BIGINT, ImmutableSet.of(0, 2, 1), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testGetReorderingAssignments()
    {
        Parameter relationRowParameter = new Parameter("%0", irType(RELATION_ROW_TYPE));
        FieldReference firstFieldReference = new FieldReference("%1", relationRowParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", relationRowParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%3", relationRowParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), thirdFieldReference.result(), secondFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), thirdFieldReference.attributes(), secondFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());
        assertThat(getReorderingAssignments(
                RELATION_ROW_TYPE,
                new FieldMapping(ImmutableMap.of(
                        1, 2,
                        2, 1,
                        0, 0)),
                new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(firstFieldReference, thirdFieldReference, secondFieldReference, rowConstructor, returnOperation)));

        // input type is not a relation row: it is relational
        assertThatThrownBy(() -> getReorderingAssignments(
                RELATION_TYPE,
                new FieldMapping(ImmutableMap.of(
                        1, 2,
                        2, 1,
                        0, 0)),
                new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");

        // input type is not a relation row: it is primitive
        assertThatThrownBy(() -> getReorderingAssignments(
                BIGINT,
                new FieldMapping(ImmutableMap.of(
                        1, 2,
                        2, 1,
                        0, 0)),
                new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");

        // field mapping is not reordering
        assertThatThrownBy(() -> getReorderingAssignments(
                RELATION_ROW_TYPE,
                new FieldMapping(ImmutableMap.of(
                        1, 2,
                        2, 1,
                        0, 2)),
                new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected reordering mapping");

        // field mapping is identity
        assertThatThrownBy(() -> getReorderingAssignments(
                RELATION_ROW_TYPE,
                new FieldMapping(ImmutableMap.of(
                        1, 1,
                        2, 2,
                        0, 0)),
                new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("attempt to create reordering projection for identity mapping");

        // empty field mapping is identity
        assertThatThrownBy(() -> getReorderingAssignments(EMPTY_ROW, FieldMapping.EMPTY, new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("attempt to create reordering projection for identity mapping");
    }

    @Test
    public void testGetEmptyFieldSelector()
    {
        // non-empty input
        Parameter relationRowParameter = new Parameter("%0", irType(RELATION_ROW_TYPE));
        Constant emptyRow = new Constant("%1", EMPTY_ROW, null);
        Return emptyReturn = new Return("%2", emptyRow.result(), emptyRow.attributes());
        assertThat(getEmptyFieldSelector("^result", RELATION_ROW_TYPE, new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(emptyRow, emptyReturn)));

        // empty input
        Parameter emptyRowParameter = new Parameter("%0", irType(EMPTY_ROW));
        assertThat(getEmptyFieldSelector("^result", EMPTY_ROW, new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(emptyRowParameter),
                        ImmutableList.of(emptyRow, emptyReturn)));

        // input type is not a relation row: it is relational
        assertThatThrownBy(() -> getEmptyFieldSelector("^result", RELATION_TYPE, new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testGetFullPassthroughFieldSelector()
    {
        // non-empty input
        Parameter relationRowParameter = new Parameter("%0", irType(RELATION_ROW_TYPE));
        FieldReference firstFieldReference = new FieldReference("%1", relationRowParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%2", relationRowParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%3", relationRowParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row allFieldsRow = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return allFieldsReturn = new Return("%5", allFieldsRow.result(), allFieldsRow.attributes());
        assertThat(getFullPassthroughFieldSelector("^result", RELATION_ROW_TYPE, new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(relationRowParameter),
                        ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, allFieldsRow, allFieldsReturn)));

        // empty input
        Parameter emptyRowParameter = new Parameter("%0", irType(EMPTY_ROW));
        Constant emptyRow = new Constant("%1", EMPTY_ROW, null);
        Return emptyReturn = new Return("%2", emptyRow.result(), emptyRow.attributes());
        assertThat(getFullPassthroughFieldSelector("^result", EMPTY_ROW, new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(new Block(
                        Optional.of("^result"),
                        ImmutableList.of(emptyRowParameter),
                        ImmutableList.of(emptyRow, emptyReturn)));

        // input type is not a relation row: it is relational
        assertThatThrownBy(() -> getFullPassthroughFieldSelector("^result", RELATION_TYPE, new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testConcatenateFieldSelectors()
    {
        // concatenate two non-empty field selectors
        // the operations results in both blocks block are re-allocated with the provided ValueNameAllocator to avoid collisions
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%4", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%5", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fifthFieldReference = new FieldReference("%6", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%9",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result(), fourthFieldReference.result(), fifthFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes(), fourthFieldReference.attributes(), fifthFieldReference.attributes()));
        Return returnOperation = new Return("%10", rowConstructor.result(), rowConstructor.attributes());
        assertThat(concatenateFieldSelectors(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, FIELD_SELECTOR_WITH_DUPLICATES), new ProgramBuilder.ValueNameAllocator()))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        // block name and parameters of the first block
                        Optional.of("^fieldSelectorWithoutDuplicates"),
                        ImmutableList.of(RELATION_ROW_PARAMETER),
                        ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, fifthFieldReference, rowConstructor, returnOperation)));

        // concatenate three empty field selectors
        assertThat(concatenateFieldSelectors(ImmutableList.of(EMPTY_FIELD_SELECTOR, EMPTY_FIELD_SELECTOR, EMPTY_FIELD_SELECTOR), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(EMPTY_FIELD_SELECTOR);

        // concatenate an empty and a non-empty field selector
        FieldReference first = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference second = new FieldReference("%1", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row row = new Row(
                "%4",
                ImmutableList.of(first.result(), second.result()),
                ImmutableList.of(first.attributes(), second.attributes()));
        Return returnOp = new Return("%5", row.result(), row.attributes());
        assertThat(concatenateFieldSelectors(ImmutableList.of(EMPTY_FIELD_SELECTOR, FIELD_SELECTOR_WITHOUT_DUPLICATES), new ProgramBuilder.ValueNameAllocator()))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        // block name and parameters of the first block
                        Optional.of("^emptyFieldSelector"),
                        ImmutableList.of(RELATION_ROW_PARAMETER),
                        ImmutableList.of(first, second, row, returnOp)));

        // concatenate one block
        assertThat(concatenateFieldSelectors(ImmutableList.of(FIELD_SELECTOR_REORDERING), new ProgramBuilder.ValueNameAllocator()))
                .isEqualTo(FIELD_SELECTOR_REORDERING);

        // concatenate no blocks
        assertThatThrownBy(() -> concatenateFieldSelectors(ImmutableList.of(), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("empty blocks list");

        // input block is not a field selector
        assertThatThrownBy(() -> concatenateFieldSelectors(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, COMPLEX_EXPRESSION_ASSIGNMENTS), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("expected field selector blocks");

        // different number of arguments for the input blocks
        assertThatThrownBy(() -> concatenateFieldSelectors(ImmutableList.of(EMPTY_FIELD_SELECTOR, EMPTY_COMPUTATION_ON_EMPTY_ROW), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("type mismatch");

        // arguments of input blocks do not match in type
        assertThatThrownBy(() -> concatenateFieldSelectors(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, MULTIPLE_PARAMETERS_BLOCK), new ProgramBuilder.ValueNameAllocator()))
                .hasMessage("all blocks must have the same number of parameters");
    }

    @Test
    public void testGetSelectedFields()
    {
        assertThat(getSelectedFields(FIELD_SELECTOR_WITHOUT_DUPLICATES))
                .isEqualTo(ImmutableList.of(2, 0));
        assertThat(getSelectedFields(FIELD_SELECTOR_WITH_DUPLICATES))
                .isEqualTo(ImmutableList.of(2, 0, 2));
        assertThat(getSelectedFields(FIELD_SELECTOR_FULL_PASSTHROUGH))
                .isEqualTo(ImmutableList.of(0, 1, 2));
        assertThat(getSelectedFields(FIELD_SELECTOR_FULL_PASSTHROUGH_WITH_DUPLICATES))
                .isEqualTo(ImmutableList.of(0, 1, 2, 1));
        assertThat(getSelectedFields(FIELD_SELECTOR_REORDERING))
                .isEqualTo(ImmutableList.of(1, 2, 0));
        assertThat(getSelectedFields(FIELD_SELECTOR_REORDERING_WITH_DUPLICATES))
                .isEqualTo(ImmutableList.of(1, 2, 0, 2));
        assertThat(getSelectedFields(EMPTY_FIELD_SELECTOR))
                .isEqualTo(ImmutableList.of());
        assertThat(getSelectedFields(EMPTY_COMPUTATION_ON_EMPTY_ROW))
                .isEqualTo(ImmutableList.of());

        assertThatThrownBy(() -> getSelectedFields(COMPLEX_EXPRESSION_ASSIGNMENTS))
                .hasMessage("expected field selector block");
        assertThatThrownBy(() -> getSelectedFields(MULTIPLE_PARAMETERS_BLOCK))
                .hasMessage("expected block with single parameter");
    }

    @Test
    public void testGetProjectedItems()
    {
        // select f2, f0 > 5, f3 from outer parameter
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference outerFieldReference = new FieldReference("%2", OUTER_RELATION_ROW_PARAMETER, 3, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constant = new Constant("%3", BIGINT, 5L);
        Comparison comparison = new Comparison("%4", firstFieldReference.result(), constant.result(), GREATER_THAN, ImmutableList.of(firstFieldReference.attributes(), constant.attributes()));
        Row rowConstructor = new Row(
                "%5",
                ImmutableList.of(thirdFieldReference.result(), comparison.result(), outerFieldReference.result()),
                ImmutableList.of(thirdFieldReference.attributes(), comparison.attributes(), outerFieldReference.attributes()));
        Return returnOperation = new Return("%6", rowConstructor.result(), rowConstructor.attributes());

        Block projectAssignments = new Block(
                Optional.of("^projectAssignments"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, thirdFieldReference, outerFieldReference, constant, comparison, rowConstructor, returnOperation));

        // first assignment: f2
        // all resulting blocks have the same name and parameter as the input block
        Return firstReturnOperation = new Return("%100", thirdFieldReference.result(), thirdFieldReference.attributes());
        Block firstProjectedItem = new Block(
                Optional.of("^projectAssignments"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(thirdFieldReference, firstReturnOperation));

        // second assignment: f0 > 5
        // all resulting blocks have the same name and parameter as the input block
        Return secondReturnOperation = new Return("%101", comparison.result(), comparison.attributes());
        Block secondProjectedItem = new Block(
                Optional.of("^projectAssignments"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, constant, comparison, secondReturnOperation));

        // third assignment: f3 from outer parameter
        // all resulting blocks have the same name and parameter as the input block
        Return thirdReturnOperation = new Return("%102", outerFieldReference.result(), outerFieldReference.attributes());
        Block thirdProjectedItem = new Block(
                Optional.of("^projectAssignments"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(outerFieldReference, thirdReturnOperation));

        assertThat(getProjectedItems(projectAssignments, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparatorForType(blockComparatorIgnoringDerivedAttributes(), Block.class)
                .isEqualTo(ImmutableList.of(firstProjectedItem, secondProjectedItem, thirdProjectedItem));

        // empty assignments
        assertThat(getProjectedItems(EMPTY_FIELD_SELECTOR, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(ImmutableList.of());

        // no project assignments
        assertThatThrownBy(() -> getProjectedItems(MULTIPLE_PARAMETERS_BLOCK, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected project assignments");
    }

    @Test
    public void testComposeProjectedItems()
    {
        // first item: %firstParameter > 5
        Parameter firstParameter = new Block.Parameter("%firstParameter", irType(BIGINT));
        Constant firstConstant = new Constant("%0", BIGINT, 5L);
        Comparison firstComparison = new Comparison("%1", firstParameter, firstConstant.result(), GREATER_THAN, ImmutableList.of(ImmutableMap.of(), firstConstant.attributes()));
        Return firstReturnOperation = new Return("%2", firstComparison.result(), firstComparison.attributes());
        Block firstItem = new Block(
                Optional.of("^firstBlock"),
                ImmutableList.of(firstParameter),
                ImmutableList.of(firstConstant, firstComparison, firstReturnOperation));

        // second item: 10 < %secondParameter
        Parameter secondParameter = new Block.Parameter("%secondParameter", irType(BIGINT));
        Constant secondConstant = new Constant("%0", BIGINT, 10L);
        Comparison secondComparison = new Comparison("%1", secondConstant.result(), secondParameter, LESS_THAN, ImmutableList.of(secondConstant.attributes(), ImmutableMap.of()));
        Return secondReturnOperation = new Return("%2", secondComparison.result(), secondComparison.attributes());
        Block secondItem = new Block(
                Optional.of("^secondBlock"),
                ImmutableList.of(secondParameter),
                ImmutableList.of(secondConstant, secondComparison, secondReturnOperation));

        // create the composed block
        // all values are remapped to avoid collisions
        Constant remappedFirstConstant = new Constant("%100", BIGINT, 5L);
        Comparison remappedFirstComparison = new Comparison("%101", firstParameter, remappedFirstConstant.result(), GREATER_THAN, ImmutableList.of(ImmutableMap.of(), remappedFirstConstant.attributes()));
        Constant remappedSecondConstant = new Constant("%103", BIGINT, 10L);
        // %secondParameter is remapped to %firstParameter
        Comparison remappedSecondComparison = new Comparison("%104", remappedSecondConstant.result(), firstParameter, LESS_THAN, ImmutableList.of(remappedSecondConstant.attributes(), ImmutableMap.of()));
        // collect items in a row
        Row rowConstructor = new Row(
                "%106",
                ImmutableList.of(remappedFirstComparison.result(), remappedSecondComparison.result()),
                ImmutableList.of(remappedFirstComparison.attributes(), remappedSecondComparison.attributes()));
        Return returnOperation = new Return("%107", rowConstructor.result(), rowConstructor.attributes());
        assertThat(composeProjectedItems(ImmutableList.of(firstItem, secondItem), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        Optional.empty(),
                        // the resulting block has the same parameters as the first component block
                        ImmutableList.of(firstParameter),
                        ImmutableList.of(remappedFirstConstant, remappedFirstComparison, remappedSecondConstant, remappedSecondComparison, rowConstructor, returnOperation)));

        // compose the same item three times
        Constant remappedFirstConstant1 = new Constant("%100", BIGINT, 5L);
        Comparison remappedFirstComparison1 = new Comparison("%101", firstParameter, remappedFirstConstant1.result(), GREATER_THAN, ImmutableList.of(ImmutableMap.of(), remappedFirstConstant1.attributes()));
        Constant remappedFirstConstant2 = new Constant("%103", BIGINT, 5L);
        Comparison remappedFirstComparison2 = new Comparison("%104", firstParameter, remappedFirstConstant2.result(), GREATER_THAN, ImmutableList.of(ImmutableMap.of(), remappedFirstConstant2.attributes()));
        Constant remappedFirstConstant3 = new Constant("%106", BIGINT, 5L);
        Comparison remappedFirstComparison3 = new Comparison("%107", firstParameter, remappedFirstConstant3.result(), GREATER_THAN, ImmutableList.of(ImmutableMap.of(), remappedFirstConstant3.attributes()));
        Row row = new Row(
                "%109",
                ImmutableList.of(
                        remappedFirstComparison1.result(),
                        remappedFirstComparison2.result(),
                        remappedFirstComparison3.result()),
                ImmutableList.of(
                        remappedFirstComparison1.attributes(),
                        remappedFirstComparison2.attributes(),
                        remappedFirstComparison3.attributes()));
        Return returnOp = new Return("%110", row.result(), row.attributes());
        assertThat(composeProjectedItems(ImmutableList.of(firstItem, firstItem, firstItem), new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new Block(
                        Optional.empty(),
                        // the resulting block has the same parameters as the first component block
                        ImmutableList.of(firstParameter),
                        ImmutableList.of(remappedFirstConstant1, remappedFirstComparison1, remappedFirstConstant2, remappedFirstComparison2, remappedFirstConstant3, remappedFirstComparison3, row, returnOp)));

        // compose assignments blocks
        // note: it isn't a concatenation. The result is a row including the component rows: ROW(ROW(f2, f0), ROW(true, f0))
        FieldReference firstFieldReferenceRemapped = new FieldReference("%100", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReferenceRemapped = new FieldReference("%101", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row firstRowConstructorRemapped = new Row(
                "%102",
                ImmutableList.of(firstFieldReferenceRemapped.result(), secondFieldReferenceRemapped.result()),
                ImmutableList.of(firstFieldReferenceRemapped.attributes(), secondFieldReferenceRemapped.attributes()));
        Constant constantTrueRemapped = new Constant("%104", BOOLEAN, true);
        FieldReference fieldReferenceRemapped = new FieldReference("%105", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row secondRowConstructorRemapped = new Row(
                "%106",
                ImmutableList.of(constantTrueRemapped.result(), fieldReferenceRemapped.result()),
                ImmutableList.of(constantTrueRemapped.attributes(), fieldReferenceRemapped.attributes()));
        Row finalRowConstructor = new Row(
                "%108",
                ImmutableList.of(firstRowConstructorRemapped.result(), secondRowConstructorRemapped.result()),
                ImmutableList.of(firstRowConstructorRemapped.attributes(), secondRowConstructorRemapped.attributes()));
        Return finalReturnOperation = new Return("%109", finalRowConstructor.result(), finalRowConstructor.attributes());
        assertThat(composeProjectedItems(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, COMPLEX_EXPRESSION_ASSIGNMENTS), new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(blockComparatorIgnoringDerivedAttributes())
                .isEqualTo(new Block(
                        Optional.empty(),
                        // the resulting block has the same parameters as the first component block
                        ImmutableList.of(RELATION_ROW_PARAMETER),
                        ImmutableList.of(firstFieldReferenceRemapped, secondFieldReferenceRemapped, firstRowConstructorRemapped, constantTrueRemapped, fieldReferenceRemapped, secondRowConstructorRemapped, finalRowConstructor, finalReturnOperation)));

        // mismatching parameters of component blocks
        assertThatThrownBy(() -> composeProjectedItems(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, EMPTY_COMPUTATION_ON_EMPTY_ROW), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("type mismatch");
        assertThatThrownBy(() -> composeProjectedItems(ImmutableList.of(FIELD_SELECTOR_WITHOUT_DUPLICATES, MULTIPLE_PARAMETERS_BLOCK), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("type mismatch");

        // empty list of component blocks
        assertThatThrownBy(() -> composeProjectedItems(ImmutableList.of(), new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("cannot combine 0 blocks");
    }

    private static Block getFieldSelectorWithoutDuplicates()
    {
        // select f2:VARCHAR, f0:BIGINT
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%2",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes()));
        Return returnOperation = new Return("%3", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorWithoutDuplicates"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorWithDuplicates()
    {
        // select f2:VARCHAR, f0:BIGINT, f2:VARCHAR
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%3",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%4", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorWithDuplicates"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorWithDeadCode()
    {
        // select f2:VARCHAR, f0:BIGINT. Includes unused selection of f1:BOOLEAN
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%3",
                ImmutableList.of(firstFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%4", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorWithDeadCode"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorWithOuterReference()
    {
        // select f2:VARCHAR, and a field f3:SMALLINT from outer parameter
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReferenceOuter = new FieldReference("%1", OUTER_RELATION_ROW_PARAMETER, 3, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%2",
                ImmutableList.of(firstFieldReference.result(), secondFieldReferenceOuter.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReferenceOuter.attributes()));
        Return returnOperation = new Return("%3", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorWithOuterReference"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReferenceOuter, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorFullPassthrough()
    {
        // select f0:BIGINT, f1:BOOLEAN, f2:VARCHAR
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%3",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%4", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorFullPassthrough"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorFullPassthroughWithDuplicates()
    {
        // select f0:BIGINT, f1:BOOLEAN, f2:VARCHAR, f1:BOOLEAN
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result(), fourthFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes(), fourthFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorFullPassthroughWithDuplicates"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorFullPassthroughWithDeadCode()
    {
        // select f0:BIGINT, f1:BOOLEAN, f2:VARCHAR. Includes unused selection of f1:BOOLEAN
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorFullPassthroughWithDeadCode"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorFullPassthroughWithOuterReference()
    {
        // select f0:BIGINT, f1:BOOLEAN, f2:VARCHAR. Includes unused selection of f3:SMALLINT from outer parameter
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", OUTER_RELATION_ROW_PARAMETER, 3, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result(), fourthFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes(), fourthFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorFullPassthroughWithOuterReference"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorFullPassthroughPrefix()
    {
        // select f0:BIGINT, f1:BOOLEAN (the first two of the three fields)
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%2",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes()));
        Return returnOperation = new Return("%3", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorFullPassthroughPrefix"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorReordering()
    {
        // select f1:BOOLEAN, f2:VARCHAR, f0:BIGINT
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%3",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%4", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorReordering"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorReorderingWithDuplicates()
    {
        // select f1:BOOLEAN, f2:VARCHAR, f0:BIGINT, f2:VARCHAR
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result(), fourthFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes(), fourthFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorReorderingWithDuplicates"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorReorderingWithDeadCode()
    {
        // select f1:BOOLEAN, f2:VARCHAR, f0:BIGINT. Includes unused selection of f2:VARCHAR
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorReorderingWithDeadCode"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getFieldSelectorReorderingWithOuterReference()
    {
        // select f1:BOOLEAN, f2:VARCHAR, f0:BIGINT.  Includes selection of f3:SMALLINT from outer parameter
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference thirdFieldReference = new FieldReference("%2", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fourthFieldReference = new FieldReference("%3", OUTER_RELATION_ROW_PARAMETER, 3, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%4",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result(), thirdFieldReference.result(), fourthFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes(), thirdFieldReference.attributes(), fourthFieldReference.attributes()));
        Return returnOperation = new Return("%5", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^fieldSelectorReorderingWithOuterReference"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, thirdFieldReference, fourthFieldReference, rowConstructor, returnOperation));
    }

    private static Block getEmptyFieldSelectorBlock()
    {
        Constant emptyRow = new Constant("%0", EMPTY_ROW, null);
        Return returnOperation = new Return("%1", emptyRow.result(), emptyRow.attributes());

        return new Block(
                Optional.of("^emptyFieldSelector"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(emptyRow, returnOperation));
    }

    private static Block getEmptyFieldSelectorWithDeadCode()
    {
        // includes unused selection of f2:VARCHAR
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant emptyRow = new Constant("%1", EMPTY_ROW, null);
        Return returnOperation = new Return("%2", emptyRow.result(), emptyRow.attributes());

        return new Block(
                Optional.of("^emptyFieldSelectorWithDeadCode"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, emptyRow, returnOperation));
    }

    private static Block getMalformedFieldSelector()
    {
        // just the Return operation
        Return returnOperation = new Return("%1", new Operation.Result("%0", irType(anonymousRow(BIGINT))), ImmutableMap.of());

        return new Block(
                Optional.of("^malformedFieldSelector"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(returnOperation));
    }

    private static Block getComplexExpressionAssignments()
    {
        // select constant true, f0:BIGINT
        Constant constantTrue = new Constant("%0", BOOLEAN, true);
        FieldReference fieldReference = new FieldReference("%1", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%2",
                ImmutableList.of(constantTrue.result(), fieldReference.result()),
                ImmutableList.of(constantTrue.attributes(), fieldReference.attributes()));
        Return returnOperation = new Return("%3", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^complexExpressionAssignments"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(constantTrue, fieldReference, rowConstructor, returnOperation));
    }

    private static Block getPrimitiveValueSelector()
    {
        // return f0:BIGINT, not wrapped in a row
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReference.result(), fieldReference.attributes());

        return new Block(
                Optional.of("^primitiveValueSelector"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, returnOperation));
    }

    private static Block getEmptyRelationalComputation()
    {
        // input: relation type. return: empty row
        Constant emptyRow = new Constant("%0", EMPTY_ROW, null);
        Return returnOperation = new Return("%1", emptyRow.result(), emptyRow.attributes());

        return new Block(
                Optional.of("^emptyRelationalComputation"),
                ImmutableList.of(RELATION_PARAMETER),
                ImmutableList.of(emptyRow, returnOperation));
    }

    private static Block getNonEmptyRelationalComputation()
    {
        // input: relation type. return: row(constant true)
        Constant constantTrue = new Constant("%0", BOOLEAN, true);
        Row rowConstructor = new Row("%1", ImmutableList.of(constantTrue.result()), ImmutableList.of(constantTrue.attributes()));
        Return returnOperation = new Return("%2", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^nonEmptyRelationalComputation"),
                ImmutableList.of(RELATION_PARAMETER),
                ImmutableList.of(constantTrue, rowConstructor, returnOperation));
    }

    private static Block getPrimitiveInputBlock()
    {
        // input: primitive type (BIGINT). return: empty row
        Constant emptyRow = new Constant("%0", EMPTY_ROW, null);
        Return returnOperation = new Return("%1", emptyRow.result(), emptyRow.attributes());

        return new Block(
                Optional.of("^primitiveInputBlock"),
                ImmutableList.of(new Parameter("%bigintParameter", irType(BIGINT))),
                ImmutableList.of(emptyRow, returnOperation));
    }

    private static Block getEmptyComputationOnEmptyRow()
    {
        Constant emptyRow = new Constant("%0", EMPTY_ROW, null);
        Return returnOperation = new Return("%1", emptyRow.result(), emptyRow.attributes());

        return new Block(
                Optional.of("^emptyComputationOnEmptyRow"),
                ImmutableList.of(EMPTY_ROW_PARAMETER),
                ImmutableList.of(emptyRow, returnOperation));
    }

    private static Block getNonEmptyComputationOnEmptyRow()
    {
        Constant constantTrue = new Constant("%0", BOOLEAN, true);
        Row rowConstructor = new Row("%1", ImmutableList.of(constantTrue.result()), ImmutableList.of(constantTrue.attributes()));
        Return returnOperation = new Return("%2", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^nonEmptyComputationOnEmptyRow"),
                ImmutableList.of(EMPTY_ROW_PARAMETER),
                ImmutableList.of(constantTrue, rowConstructor, returnOperation));
    }

    private static Block getMultipleParametersBlock()
    {
        // select f1:BOOLEAN, from the first parameter and f3:SMALLINT from the second parameter
        FieldReference firstFieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference secondFieldReference = new FieldReference("%1", OUTER_RELATION_ROW_PARAMETER, 3, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowConstructor = new Row(
                "%2",
                ImmutableList.of(firstFieldReference.result(), secondFieldReference.result()),
                ImmutableList.of(firstFieldReference.attributes(), secondFieldReference.attributes()));
        Return returnOperation = new Return("%3", rowConstructor.result(), rowConstructor.attributes());

        return new Block(
                Optional.of("^multipleParametersBlock"),
                ImmutableList.of(RELATION_ROW_PARAMETER, OUTER_RELATION_ROW_PARAMETER),
                ImmutableList.of(firstFieldReference, secondFieldReference, rowConstructor, returnOperation));
    }
}
