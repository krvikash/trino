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
package io.trino.sql.planner.exploratory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.MultisetType;
import io.trino.sql.dialect.ir.IrDialect.FunctionType;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoGroupMatcher.memoGroup;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_MEMO_OPERATION;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.GroupChildMatcher.groupChild;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.ParameterChildMatcher.parameterChild;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.memoOperation;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.query;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.singleConstantRow;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroup;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroups;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroupsContains;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroupsDoesNotContain;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestMemo
{
    private static final ResolvedFunction LESS_THAN_BIGINT = new TestingFunctionResolution().resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(BIGINT, BIGINT));

    private static final ResolvedFunction LESS_THAN_OR_EQUAL_BIGINT = new TestingFunctionResolution().resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testSimpleProgram()
    {
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // test overall memo structure
        assertMemoGroups(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        1, memoGroup() // "%row"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        2, memoGroup() // "%return"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build(),
                        3, memoGroup() // "%values"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("values")
                                        .withChildren(groupChild(2))
                                        .build())
                                .build(),
                        4, memoGroup() // "%field_reference_output"
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)))
                                .withOperations(memoOperation()
                                        .withName("field_reference")
                                        .withChildren(parameterChild(0))
                                        .build())
                                .build(),
                        5, memoGroup() // "%row_output"
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)))
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(4))
                                        .build())
                                .build(),
                        6, memoGroup() // "%return_output"
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)))
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(5))
                                        .build())
                                .build(),
                        7, memoGroup() // "%output"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("output")
                                        .withChildren(groupChild(3), groupChild(6))
                                        .build())
                                .build(),
                        8, memoGroup() // "%query"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("query")
                                        .withChildren(groupChild(7))
                                        .build())
                                .build()));

        // assert root group
        assertThat(memo.rootGroup()).isEqualTo(8);

        // inspect the group of the Output operation in detail
        assertMemoGroup(
                memo.getGroup(7), // "%output"
                memoGroup()
                        .withResultType(irType(BOOLEAN))
                        .withGroupParameterTypes()
                        .withOperations(memoOperation()
                                .withDialect(TRINO)
                                .withOperationId(new OperationId(
                                        "output",
                                        ImmutableList.of(irType(new MultisetType(anonymousRow(BIGINT)))),
                                        ImmutableList.of(new Type(IR, new FunctionType(ImmutableList.of(irType(anonymousRow(BIGINT))), irType(anonymousRow(BIGINT)))))))
                                .withGroupParameterTypes()
                                .withChildren(groupChild(3), groupChild(6))
                                .withAttributes(attributes(
                                        new AttributeKey(TRINO, "output:column_names"),
                                        ImmutableList.of("output_column"),
                                        new AttributeKey(IR, TERMINAL),
                                        true,
                                        new AttributeKey(IR, REPEATABILITY),
                                        DETERMINISTIC,
                                        new AttributeKey(IR, SAFE),
                                        true,
                                        new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                        true))
                                .build())
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                true,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                true))
                        .build());
    }

    @Test
    public void testGroupNames()
    {
        // Values operation has three Blocks, each producing a single row with a single constant column
        Values valuesOperation = valuesOfRows(
                ImmutableList.of(
                        singleConstantRow("", BIGINT, 1L),
                        singleConstantRow("", BIGINT, 2L),
                        singleConstantRow("", BIGINT, 3L)));
        Program program = program("", valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroups(
                memo.groups(),
                ImmutableMap.<Integer, MemoGroupMatcher>builder()
                        .put(0, memoGroup() // first "%constant"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build())
                        .put(1, memoGroup() // first "%row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build())
                        .put(2, memoGroup() // first "%return"
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build())
                        .put(3, memoGroup() // second "%constant".
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build())
                        .put(4, memoGroup() // second "%row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(3))
                                        .build())
                                .build())
                        .put(5, memoGroup() // second "%return"
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(4))
                                        .build())
                                .build())
                        .put(6, memoGroup() // third "%constant"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build())
                        .put(7, memoGroup() // third "%row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(6))
                                        .build())
                                .build())
                        .put(8, memoGroup() // third "%return"
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(7))
                                        .build())
                                .build())
                        .put(9, memoGroup() // "%values"
                                .withOperations(memoOperation()
                                        .withName("values")
                                        .withChildren(groupChild(2), groupChild(5), groupChild(8))
                                        .build())
                                .build())
                        .put(10, memoGroup() // "%field_reference"
                                .withOperations(memoOperation()
                                        .withName("field_reference")
                                        .withChildren(parameterChild(0))
                                        .build())
                                .build())
                        .put(11, memoGroup() // fourth "%row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(10))
                                        .build())
                                .build())
                        .put(12, memoGroup() // fourth "%return"
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(11))
                                        .build())
                                .build())
                        .put(13, memoGroup() // "%output"
                                .withOperations(memoOperation()
                                        .withName("output")
                                        .withChildren(groupChild(9), groupChild(12))
                                        .build())
                                .build())
                        .put(14, memoGroup() // "%query"
                                .withOperations(memoOperation()
                                        .withName("query")
                                        .withChildren(groupChild(13))
                                        .build())
                                .build())
                        .buildOrThrow());
    }

    @Test
    public void testDeduplication()
    {
        // Both rows have identical code
        Values valuesOperation = valuesOfRows(
                ImmutableList.of(
                        singleConstantRow("1", BIGINT, 1L),
                        singleConstantRow("2", BIGINT, 1L)));

        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // deduplicated Constant operations from both rows
                        0, memoGroup() // "%constant1" and "%constant2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // deduplicated Row operations from both rows
                        1, memoGroup() // "%row1" and "%row2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        // deduplicated Return operations from both rows
                        2, memoGroup() // "%return1" and "%return2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build(),
                        3, memoGroup() // "%values"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("values")
                                        // both rows refer to the same group id 2, but different lineage (2 and 5)
                                        .withChildren(groupChild(2), groupChild(2))
                                        .build())
                                .build()));
    }

    @Test
    public void testOneChildDeduplication()
    {
        // first row
        Constant constantOperation1 = new Constant("%constant1", BIGINT, 1L);
        Constant constantOperation2 = new Constant("%constant2", BIGINT, 2L);
        Call comparisonOperation1 = new Call("%comparison1", ImmutableList.of(constantOperation1.result(), constantOperation2.result()), LESS_THAN_BIGINT, ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        Row rowOperation1 = new Row("%row1", ImmutableList.of(comparisonOperation1.result()), ImmutableList.of(comparisonOperation1.attributes()));
        Return returnOperation1 = new Return("%return1", rowOperation1.result(), rowOperation1.attributes());
        List<Operation> firstRowOperations = ImmutableList.of(constantOperation1, constantOperation2, comparisonOperation1, rowOperation1, returnOperation1);

        // second row
        Constant constantOperation3 = new Constant("%constant3", BIGINT, 1L);
        Constant constantOperation4 = new Constant("%constant4", BIGINT, 3L);
        Call comparisonOperation2 = new Call("%comparison2", ImmutableList.of(constantOperation3.result(), constantOperation4.result()), LESS_THAN_BIGINT, ImmutableList.of(constantOperation3.attributes(), constantOperation4.attributes()));
        Row rowOperation2 = new Row("%row2", ImmutableList.of(comparisonOperation2.result()), ImmutableList.of(comparisonOperation2.attributes()));
        Return returnOperation2 = new Return("%return2", rowOperation2.result(), rowOperation2.attributes());
        List<Operation> secondRowOperations = ImmutableList.of(constantOperation3, constantOperation4, comparisonOperation2, rowOperation2, returnOperation2);

        Values valuesOperation = valuesOfRows(ImmutableList.of(firstRowOperations, secondRowOperations));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // deduplicated 'Constant 1' operations from both rows
                        0, memoGroup() // "%constant1" and "%constant3"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        1, memoGroup() // "%constant2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        5, memoGroup() // "%constant4"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // Comparison operations are not deduplicated because their second child is different
                        2, memoGroup() // "%comparison1"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("call")
                                        .withChildren(groupChild(0), groupChild(1))
                                        .build())
                                .build(),
                        6, memoGroup() // "%comparison2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("call")
                                        .withChildren(groupChild(0), groupChild(5))
                                        .build())
                                .build()));
    }

    @Test
    public void testAllChildrenDeduplication()
    {
        // first row
        Constant constantOperation1 = new Constant("%constant1", BIGINT, 1L);
        Constant constantOperation2 = new Constant("%constant2", BIGINT, 2L);
        Call comparisonOperation1 = new Call("%comparison1", ImmutableList.of(constantOperation1.result(), constantOperation2.result()), LESS_THAN_BIGINT, ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        Row rowOperation1 = new Row("%row1", ImmutableList.of(comparisonOperation1.result()), ImmutableList.of(comparisonOperation1.attributes()));
        Return returnOperation1 = new Return("%return1", rowOperation1.result(), rowOperation1.attributes());
        List<Operation> firstRowOperations = ImmutableList.of(constantOperation1, constantOperation2, comparisonOperation1, rowOperation1, returnOperation1);

        // second row
        Constant constantOperation3 = new Constant("%constant3", BIGINT, 1L);
        Constant constantOperation4 = new Constant("%constant4", BIGINT, 2L);
        Call comparisonOperation2 = new Call("%comparison2", ImmutableList.of(constantOperation3.result(), constantOperation4.result()), LESS_THAN_OR_EQUAL_BIGINT, ImmutableList.of(constantOperation3.attributes(), constantOperation4.attributes()));
        Row rowOperation2 = new Row("%row2", ImmutableList.of(comparisonOperation2.result()), ImmutableList.of(comparisonOperation2.attributes()));
        Return returnOperation2 = new Return("%return2", rowOperation2.result(), rowOperation2.attributes());
        List<Operation> secondRowOperations = ImmutableList.of(constantOperation3, constantOperation4, comparisonOperation2, rowOperation2, returnOperation2);

        Values valuesOperation = valuesOfRows(ImmutableList.of(firstRowOperations, secondRowOperations));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // deduplicated 'Constant 1' operations from both rows
                        0, memoGroup() // "%constant1" and "%constant3"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // deduplicated 'Constant 2' operations from both rows
                        1, memoGroup() // "%constant2" and "%constant4"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // call operations are not deduplicated because their functions differ ($less_than vs $less_than_or_equal), even though both children are the same
                        2, memoGroup() // "%comparison1"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("call")
                                        .withChildren(groupChild(0), groupChild(1))
                                        .build())
                                .build(),
                        5, memoGroup() // "%comparison2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("call")
                                        .withChildren(groupChild(0), groupChild(1))
                                        .build())
                                .build()));
    }

    @Test
    public void testProperParameterLineage()
    {
        // Join operation has two sources and six blocks. it passes proper parameters to each block.
        // left source of a Join
        Values valuesOperation1 = valuesOfRows("1", ImmutableList.of(singleConstantRow("1", BIGINT, 42L)));
        // right source of a Join
        Values valuesOperation2 = valuesOfRows("2", ImmutableList.of(singleConstantRow("2", BOOLEAN, true)));

        Parameter leftCriteriaParameter = new Parameter("%leftCriteriaParameter", irType(anonymousRow(BIGINT)));
        Parameter rightCriteriaParameter = new Parameter("%rightCriteriaParameter", irType(anonymousRow(BOOLEAN)));
        Parameter leftFilterParameter = new Parameter("%leftFilterParameter", irType(anonymousRow(BIGINT)));
        Parameter rightFilterParameter = new Parameter("%rightFilterParameter", irType(anonymousRow(BOOLEAN)));
        Parameter leftOutputParameter = new Parameter("%leftOutputParameter", irType(anonymousRow(BIGINT)));
        Parameter rightOutputParameter = new Parameter("%rightOutputParameter", irType(anonymousRow(BOOLEAN)));
        Parameter dynamicFilterParameter = new Parameter("%dynamicFilterParameter", irType(anonymousRow(BOOLEAN)));

        Constant emptyRowConstant = new Constant("%empty_row", EMPTY_ROW, null);
        Return returnEmptyRow = new Return("%return_empty_row", emptyRowConstant.result(), emptyRowConstant.attributes());

        FieldReference fieldReferenceOperation1 = new FieldReference("%field_reference1", rightFilterParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnFieldReference1 = new Return("%return_field_reference1", fieldReferenceOperation1.result(), fieldReferenceOperation1.attributes());

        FieldReference fieldReferenceOperation2 = new FieldReference("%field_reference2", leftOutputParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation3 = new Row("%row", ImmutableList.of(fieldReferenceOperation2.result()), ImmutableList.of(fieldReferenceOperation2.attributes()));
        Return returnFieldReference2 = new Return("%return_field_reference2", rowOperation3.result(), rowOperation3.attributes());

        Join joinOperation = new Join(
                "%join",
                valuesOperation1.result(),
                valuesOperation2.result(),
                new Block(
                        Optional.of("^leftCriteriaSelector"),
                        ImmutableList.of(leftCriteriaParameter), // proper parameter 0
                        ImmutableList.of(emptyRowConstant, returnEmptyRow)),
                new Block(
                        Optional.of("^rightCriteriaSelector"),
                        ImmutableList.of(rightCriteriaParameter), // proper parameter 1
                        ImmutableList.of(emptyRowConstant, returnEmptyRow)),
                new Block(
                        Optional.of("^filter"),
                        ImmutableList.of(leftFilterParameter, rightFilterParameter), // proper parameters 2 and 3
                        ImmutableList.of(fieldReferenceOperation1, returnFieldReference1)),
                new Block(
                        Optional.of("^leftOutputSelector"),
                        ImmutableList.of(leftOutputParameter), // proper parameter 4
                        ImmutableList.of(fieldReferenceOperation2, rowOperation3, returnFieldReference2)),
                new Block(
                        Optional.of("^rightOutputSelector"),
                        ImmutableList.of(rightOutputParameter), // proper parameter 5
                        ImmutableList.of(emptyRowConstant, returnEmptyRow)),
                new Block(
                        Optional.of("^dynamicFilterTargetSelector"),
                        ImmutableList.of(dynamicFilterParameter), // proper parameter 6
                        ImmutableList.of(emptyRowConstant, returnEmptyRow)),
                JoinOperationMetadata.JoinType.LEFT,
                false,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                valuesOperation1.attributes(),
                valuesOperation2.attributes());

        Program program = program(ImmutableList.of(valuesOperation1, valuesOperation2, joinOperation));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        17, memoGroup() // "%join"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withGroupParameterTypes()
                                        .withName("join")
                                        .withChildren(
                                                groupChild(
                                                        // left source
                                                        3,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of())),
                                                groupChild(
                                                        // right source
                                                        7,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of())),
                                                groupChild(
                                                        // left criteria selector
                                                        9,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(0))),
                                                groupChild(
                                                        // right criteria selector
                                                        11,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(1))),
                                                groupChild(
                                                        // filter
                                                        13,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(2, 3))),
                                                groupChild(
                                                        // left output selector
                                                        16,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(4))),
                                                groupChild(
                                                        // right output selector
                                                        11,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(5))),
                                                groupChild(
                                                        // dynamic filter target selector
                                                        11,
                                                        new ParameterLineage(ImmutableList.of(), ImmutableList.of(6))))
                                        .build())
                                .build()));
    }

    @Test
    public void testAccessOuterScopeResult()
    {
        // Constant operation is defined before the Values operation. it is referenced from within the Values operation block.
        Constant constantOperation = new Constant("%constant", BIGINT, 42L);

        Row rowOperation1 = new Row("%row1", ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation1 = new Return("%return1", rowOperation1.result(), rowOperation1.attributes());
        List<Operation> rowOperations = ImmutableList.of(rowOperation1, returnOperation1);

        Values valuesOperation = valuesOfRows(ImmutableList.of(rowOperations));

        Program program = program(ImmutableList.of(constantOperation, valuesOperation));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // the created group structure is the same as in testSimpleProgram, despite constant being defined outside of Values block
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        1, memoGroup() // "%row1"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build()));
    }

    @Test
    public void testParameterLineage()
    {
        // nested lambdas are defined before the Values operation. They are effectively dead code, but show up in Memo.
        // The inner lambda returns its outer lambda parameter.
        Parameter innerLambdaParameter = new Parameter("%innerLambdaParameter", irType(anonymousRow(BOOLEAN)));
        Parameter outerLambdaParameter = new Parameter("%outerLambdaParameter", irType(anonymousRow(BIGINT)));
        Lambda innerLambdaOperation = new Lambda(
                "%inner_lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(innerLambdaParameter),
                        ImmutableList.of(new Return("%inner_lambda_return", outerLambdaParameter, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES))));
        Lambda outerLambdaOperation = new Lambda(
                "%outer_lambda",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(outerLambdaParameter),
                        ImmutableList.of(innerLambdaOperation, new Return("%outer_lambda_return", outerLambdaParameter, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES))));
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("values", BIGINT, 42L)));
        Program program = program(ImmutableList.of(outerLambdaOperation, valuesOperation));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        1, memoGroup() // "%inner_lambda"
                                // the passed parameter type: row(BIGINT) (the proper parameter of outer lambda)
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)))
                                .withOperations(memoOperation()
                                        .withName("lambda")
                                        .withChildren(groupChild(
                                                0,
                                                // the inner lambda passes to its body the outer parameter and its proper parameter
                                                new ParameterLineage(ImmutableList.of(0), ImmutableList.of(0))))
                                        .build())
                                .build(),
                        0, memoGroup() // "%inner_lambda_return"
                                // the passed parameter types: row(BIGINT) (from outer lambda) and row(BOOLEAN) (from inner lambda)
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)), irType(anonymousRow(BOOLEAN)))
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(parameterChild(0))
                                        .build())
                                .build(),
                        3, memoGroup() // "%outer_lambda"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("lambda")
                                        .withChildren(groupChild(
                                                2,
                                                // there are no outer parameters to pass. the outer lambda passes to its body its proper parameter
                                                new ParameterLineage(ImmutableList.of(), ImmutableList.of(0))))
                                        .build())
                                .build(),
                        2, memoGroup() // "%outer_lambda_return"
                                // the passed parameter type: row(BIGINT) (the proper parameter of outer lambda)
                                .withGroupParameterTypes(irType(anonymousRow(BIGINT)))
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(parameterChild(0))
                                        .build())
                                .build()));
    }

    @Test
    public void testMergeGroupIntoItself()
    {
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        MemoGroupMatcher constantGroupMatcher = memoGroup()
                .withOperations(memoOperation()
                        .withName("constant")
                        .withResultType(irType(BIGINT))
                        .withAttributes(attributes(
                                new AttributeKey(TRINO, "constant:value"),
                                ConstantValue.of(BIGINT, 42L),
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                true,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                false))
                        .build())
                .build();

        assertMemoGroupsContains(memo.groups(), ImmutableMap.of(0, constantGroupMatcher)); // "%constant"

        // merging a group into itself is a no-op
        int mergedId = memo.mergeGroups(0, 0);
        assertThat(mergedId).isEqualTo(0);
        assertThat(memo.getRecentId(0)).isEqualTo(0);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(memo.groups(), ImmutableMap.of(0, constantGroupMatcher));
    }

    @Test
    public void testMergeGroupsFailsWhenItWouldCreateCycle()
    {
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));

        Parameter assignmentsParameter = new Parameter("%assignments_parameter", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperation = new FieldReference("%field_reference_assignments", assignmentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%row_assignments", ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%return_assignments", rowOperation.result(), rowOperation.attributes());
        Project projectOperation = new Project(
                "%project",
                valuesOperation.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(fieldReferenceOperation, rowOperation, returnOperation)),
                valuesOperation.attributes());

        Program program = program(ImmutableList.of(valuesOperation, projectOperation));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        3, memoGroup() // "%values"
                                .withOperations(memoOperation()
                                        .withName("values")
                                        .withChildren(groupChild(2))
                                        .build())
                                .build(),
                        7, memoGroup() // "%project"
                                .withOperations(memoOperation()
                                        .withName("project")
                                        .withChildren(
                                                groupChild(3), // "%values"
                                                groupChild(6))
                                        .build())
                                .build()));

        assertThatThrownBy(() -> memo.mergeGroups(7, 3))
                .hasMessage("Merging groups would create a cycle");
    }

    @Test
    public void testMergeGroupsRecursively()
    {
        // Both rows have identical code but cannot be deduplicated because HyperLogLog type is not comparable.
        Values valuesOperation = valuesOfRows(
                ImmutableList.of(
                        singleConstantRow("1", HYPER_LOG_LOG, EMPTY_SLICE),
                        singleConstantRow("2", HYPER_LOG_LOG, EMPTY_SLICE)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant1"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        1, memoGroup() // "%row1"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        2, memoGroup() // "%return1"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build(),
                        3, memoGroup() // "%constant2"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        4, memoGroup() // "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(3))
                                        .build())
                                .build(),
                        5, memoGroup() // "%return2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(4))
                                        .build())
                                .build(),
                        6, memoGroup() // "%values"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("values")
                                        .withChildren(groupChild(2), groupChild(5))
                                        .build())
                                .build()));

        // merge the two identical but not deduplicated constant groups
        int mergedId = memo.mergeGroups(0, 3);
        assertThat(mergedId).isEqualTo(0);
        memo.validateGroupToParentsMapping();
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // merged constant group
                        0, memoGroup() // "%constant1" and "%constant2"
                                .withOperations(
                                        // operations within the merged group still cannot be deduplicated
                                        memoOperation()
                                                .withName("constant")
                                                .withChildren()
                                                .build(),
                                        memoOperation()
                                                .withName("constant")
                                                .withChildren()
                                                .build())
                                .build(),
                        // parent groups recursively merged since they refer to the merged constant group
                        1, memoGroup() // "%row1" and "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        // parent groups recursively merged since they refer to the merged row group
                        2, memoGroup() // "%return1" and "%return2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build(),
                        6, memoGroup() // "%values"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("values")
                                        // both rows now refer to the same %return1 group
                                        .withChildren(groupChild(2), groupChild(2))
                                        .build())
                                .build()));

        assertThat(memo.getRecentId(3)).isEqualTo(0);
        assertThat(memo.getRecentId(4)).isEqualTo(1);
        assertThat(memo.getRecentId(5)).isEqualTo(2);
        assertMemoGroupsDoesNotContain(memo.groups(), 3, 4, 5); // "%constant2", "%row2", and "%return2"
    }

    @Test
    public void testMergeNonLeafGroups()
    {
        // Both rows have identical code but cannot be deduplicated because HyperLogLog type is not comparable.
        Values valuesOperation = valuesOfRows(
                ImmutableList.of(
                        singleConstantRow("1", HYPER_LOG_LOG, EMPTY_SLICE),
                        singleConstantRow("2", HYPER_LOG_LOG, EMPTY_SLICE)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // merge the two groups corresponding to the two identical row operations without merging their children (constants)
        int mergedId = memo.mergeGroups(1, 4); // "%row1" and "%row2"
        assertThat(mergedId).isEqualTo(1);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // constant groups are not merged
                        0, memoGroup() // "%constant1"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        3, memoGroup() // "%constant2"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // merged row group
                        1, memoGroup() // "%row1" and "%row2"
                                .withOperations(
                                        memoOperation()
                                                .withName("row")
                                                .withChildren(groupChild(0))
                                                .build(),
                                        memoOperation()
                                                .withName("row")
                                                .withChildren(groupChild(3))
                                                .build())
                                .build(),
                        // parent groups recursively merged since they refer to the merged row group
                        2, memoGroup() // "%return1" and "%return2"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("return")
                                        .withChildren(groupChild(1))
                                        .build())
                                .build(),
                        6, memoGroup() // "%values"
                                .withGroupParameterTypes()
                                .withOperations(memoOperation()
                                        .withName("values")
                                        // both rows now refer to the same %return1 group
                                        .withChildren(groupChild(2), groupChild(2))
                                        .build())
                                .build()));

        assertThat(memo.getRecentId(4)).isEqualTo(1);
        assertThat(memo.getRecentId(5)).isEqualTo(2);
        assertMemoGroupsDoesNotContain(memo.groups(), 4, 5); // "%row2" and "%return2"

        // remove one of the row operations from the merged row group
        memo.removeOperation(memo.getGroup(1).operations().get(1));
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant1"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // constant2 group remains in Memo, but it is not referenced anymore
                        3, memoGroup() // "%constant2"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withChildren()
                                        .build())
                                .build(),
                        // merged row group after removing one of the row operations
                        1, memoGroup()
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build()));

        // remove the unreferenced constant2 group
        memo.pruneOrphanedGroups();
        memo.validateGroupToParentsMapping();
        assertMemoGroupsDoesNotContain(memo.groups(), 3);
    }

    @Test
    public void testMergeRootGroup()
    {
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));

        // add an alternative Query operation. Initially, it is not the root operation, but dead code.
        Values anotherValuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 0L)));
        Query anotherQuery = query("_another", anotherValuesOperation);

        Program program = program(ImmutableList.of(anotherQuery, valuesOperation));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        8, memoGroup().build(), // "%query_another" group for the alternative Query operation
                        14, memoGroup().build())); // "%query" group for the main Query operation
        assertThat(memo.rootGroup()).isEqualTo(14);

        // merge the root group into the other group. the other group becomes the new root.
        int mergedId = memo.mergeGroups(8, 14);
        assertThat(mergedId).isEqualTo(8);
        assertThat(memo.getRecentId(14)).isEqualTo(8);
        memo.validateGroupToParentsMapping();
        assertMemoGroupsDoesNotContain(memo.groups(), 14);
        assertThat(memo.rootGroup()).isEqualTo(8);
    }

    /**
     * Upon merging groups, the Memo DAG  has to be updated downstream to fix all references to the merged groups and merge parent groups recursively if needed.
     * Due to the DAG structure, some operations may need to be updated multiple times during the merge process.
     * <p>
     * This test ensures that such multiple updates are handled correctly.
     * <p>
     * The program involves:
     * <ul>
     * <li>Two identical constant groups (constant2 and constant3) that are deduplicated</li>
     * <li>Another constant group (constant1)</li>
     * <li>Two row groups (row1 and row2), each referencing one of the constant groups constant1 and constant2 respectively</li>
     * <li>A parent row group (parentRow) referencing row2 and constant3</li>
     * </ul>
     * When we enforce merging constant1 and constant2, row1 and row2 should be merged (both referencing the same constant group),
     * and parentRow should be updated to reference the merged row group (instead of row2) and merged constant group (instead of constant3).
     * <pre>
     * row1
     * └── constant1
     * </pre>
     * <pre>
     * parentRow
     * ├── row2
     * │   └── constant2
     * └── constant3
     * </pre>
     */
    @Test
    public void testUpdateOperationMultipleTimesOnMergeV1()
    {
        List<Operation> row1Operations = singleConstantRow("1", BIGINT, 0L);
        Values valuesOperation1 = valuesOfRows(ImmutableList.of(row1Operations));

        List<Operation> row2Operations = singleConstantRow("2", BIGINT, 1L);
        Row row2 = (Row) row2Operations.get(1);
        Constant constant3 = new Constant("%constant3", BIGINT, 1L);
        Row parentRow = new Row(
                "%parent_row",
                ImmutableList.of(row2.result(), constant3.result()),
                ImmutableList.of(row2.attributes(), constant3.attributes()));
        Return parentReturn = new Return("%parent_return", parentRow.result(), parentRow.attributes());
        Values valuesOperation2 = valuesOfRows(
                ImmutableList.of(
                        ImmutableList.<Operation>builder()
                                .addAll(row2Operations.subList(0, row2Operations.size() - 1)) // skip return
                                .add(constant3)
                                .add(parentRow)
                                .add(parentReturn)
                                .build()));

        Program program = program(ImmutableList.of(valuesOperation1, valuesOperation2));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // assert the initial structure
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup().build(), // "%constant1"
                        1, memoGroup() // "%row1"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        4, memoGroup().build(), // "%constant2" and "%constant3"
                        5, memoGroup() // "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(4))
                                        .build())
                                .build(),
                        6, memoGroup() // "%parent_row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(
                                                groupChild(5),
                                                groupChild(4))
                                        .build())
                                .build()));

        // merge the two constant groups
        // The sequence of updates should be: (skipping Return operation for simplicity)
        // 1. merge constant2 -> constant1; groupId mapping {%constant2 -> %constant1}
        // 2. enqueue row2 and parentRow for update since they both reference constant2
        // 3. dequeue row2 for update processing, apply groupId mapping, get operation identical to row1
        // 4. merge row2 -> row1; groupId mapping {%constant2 -> %constant1, %row2 -> %row1}
        // 5. enqueue parentRow for update again since it references row2
        // 6. dequeue parentRow for update processing, apply groupId mapping, get updated parentRow referencing row1 and constant1
        // 7. dequeue parentRow for update processing again, but no changes needed this time because we find that the most recent version is fully updated
        // 8. queue is empty, finish processing
        int mergedId = memo.mergeGroups(0, 4); // "%constant1" and "%constant2" and "%constant3"
        assertThat(mergedId).isEqualTo(0);
        memo.validateGroupToParentsMapping();

        // assert the updated structure
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // merged constant group
                        0, memoGroup() // "%constant1", "%constant2" and "%constant3"
                                .withOperations(
                                        memoOperation().withName("constant").build(),
                                        memoOperation().withName("constant").build())
                                .build(),
                        // merged row group
                        1, memoGroup() // "%row1" and "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        // parent row updated to reference the merged row and constant groups
                        6, memoGroup() // "%parent_row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(
                                                groupChild(1),
                                                groupChild(0))
                                        .build())
                                .build()));

        assertThat(memo.getRecentId(4)).isEqualTo(0);
        assertThat(memo.getRecentId(5)).isEqualTo(1);
        assertMemoGroupsDoesNotContain(memo.groups(), 4, 5); // "%constant2" and "%row2"
    }

    /**
     * This test case is similar to the previous one, but parentRow takes children in different order.
     * It results in a different sequence of updates during the merge process.
     * TODO currently the operations to update are always processed in the same order, reflecting the insertion order into the Memo.
     *    This should change in the future when we implement a more efficient solution to identify parent operations for update.
     * <pre>
     * row1
     * └── constant1
     * </pre>
     * <pre>
     * parentRow
     * ├── constant3
     * └── row2
     *     └── constant2
     * </pre>
     * After merging constant1 and constant2:
     */
    @Test
    public void testUpdateOperationMultipleTimesOnMergeV2()
    {
        List<Operation> row1Operations = singleConstantRow("1", BIGINT, 0L);
        Values valuesOperation1 = valuesOfRows(ImmutableList.of(row1Operations));

        List<Operation> row2Operations = singleConstantRow("2", BIGINT, 1L);
        Row row2 = (Row) row2Operations.get(1);
        Constant constant3 = new Constant("%constant3", BIGINT, 1L);
        Row parentRow = new Row(
                "%parent_row",
                ImmutableList.of(constant3.result(), row2.result()),
                ImmutableList.of(constant3.attributes(), row2.attributes()));
        Return parentReturn = new Return("%parent_return", parentRow.result(), parentRow.attributes());
        Values valuesOperation2 = valuesOfRows(
                ImmutableList.of(
                        ImmutableList.<Operation>builder()
                                .addAll(row2Operations.subList(0, row2Operations.size() - 1)) // skip return
                                .add(constant3)
                                .add(parentRow)
                                .add(parentReturn)
                                .build()));

        Program program = program(ImmutableList.of(valuesOperation1, valuesOperation2));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // assert the initial structure
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup().build(), // "%constant1"
                        1, memoGroup() // "%row1"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        4, memoGroup().build(), // "%constant2" and "%constant3"
                        5, memoGroup() // "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(4))
                                        .build())
                                .build(),
                        6, memoGroup() // "%parent_row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(
                                                groupChild(4),
                                                groupChild(5))
                                        .build())
                                .build()));

        // merge the two constant groups
        // The sequence of updates should be: (skipping Return operation for simplicity)
        // 1. merge constant2 -> constant1; groupId mapping {%constant2 -> %constant1}
        // 2. enqueue parentRow and row2 for update since they both reference constant2
        // 3. dequeue parentRow for update processing, apply groupId mapping, get operation with one child updated (constant2 -> constant1)
        // 4. dequeue row2 for update processing, apply groupId mapping, get operation identical to row1
        // 5. merge row2 -> row1; groupId mapping {%constant2 -> %constant1, %row2 -> %row1}
        // 6. enqueue parentRow for update again since it references row2
        // 7. dequeue parentRow for update processing, find the most recent version referencing constant1, apply groupId mapping, and get updated parentRow referencing row1 and constant1
        // 8. queue is empty, finish processing
        int mergedId = memo.mergeGroups(0, 4); // "%constant1" and "%constant2"
        assertThat(mergedId).isEqualTo(0);
        memo.validateGroupToParentsMapping();

        // assert the updated structure
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        // merged constant group
                        0, memoGroup() // "%constant1" and "%constant2" and "%constant3"
                                .withOperations(
                                        memoOperation().withName("constant").build(),
                                        memoOperation().withName("constant").build())
                                .build(),
                        // merged row group
                        1, memoGroup() // "%row1" and "%row2"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(groupChild(0))
                                        .build())
                                .build(),
                        // parent row updated to reference the merged row and constant groups
                        6, memoGroup() // "%parent_row"
                                .withOperations(memoOperation()
                                        .withName("row")
                                        .withChildren(
                                                groupChild(0),
                                                groupChild(1))
                                        .build())
                                .build()));

        assertThat(memo.getRecentId(4)).isEqualTo(0);
        assertThat(memo.getRecentId(5)).isEqualTo(1);
        assertMemoGroupsDoesNotContain(memo.groups(), 4, 5); // "%constant2" and "%row2"
    }

    @Test
    public void testInsertOperation()
    {
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // assert existing operation
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withAttributes(attributes(
                                                new AttributeKey(TRINO, "constant:value"),
                                                ConstantValue.of(BIGINT, 42L),
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false))
                                        .build())
                                .build()));

        // new operation identical to the constant operation in the program
        MemoOperation memoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect(TRINO)
                .withOperationId(new OperationId("constant", ImmutableList.of(), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withResultType(irType(BIGINT))
                .withChildren(ImmutableList.of())
                .withAttributes(attributes(
                        new AttributeKey(TRINO, "constant:value"),
                        ConstantValue.of(BIGINT, 42L),
                        new AttributeKey(IR, REPEATABILITY),
                        DETERMINISTIC,
                        new AttributeKey(IR, SAFE),
                        true,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS),
                        false))
                .withInherentOperationAttributeKeys(ImmutableSet.of(new AttributeKey(TRINO, "constant:value")))
                .build();

        // inserting an identical operation returns the existing group's ID
        assertThat(memo.insertOperation(memoOperation))
                .isEqualTo(0);
        memo.validateGroupToParentsMapping();

        // new operation not yet present in the program
        memoOperation = MemoOperationBuilder.from(memoOperation)
                .withAttributes(attributes(
                        new AttributeKey(TRINO, "constant:value"),
                        ConstantValue.of(BOOLEAN, true),
                        new AttributeKey(IR, REPEATABILITY),
                        DETERMINISTIC,
                        new AttributeKey(IR, SAFE),
                        true,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS),
                        false))
                .build();

        // inserting a new operation returns a new group ID
        assertThat(memo.insertOperation(memoOperation))
                .isEqualTo(9);
        memo.validateGroupToParentsMapping();
    }

    @Test
    public void testFailedRemoveOperation()
    {
        // successful removal of an existing operation is tested in testMergeNonLeafGroups
        Values valuesOperation = valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)));
        Program program = program(valuesOperation);
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // assert existing operation
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        0, memoGroup() // "%constant"
                                .withOperations(memoOperation()
                                        .withName("constant")
                                        .withAttributes(attributes(
                                                new AttributeKey(TRINO, "constant:value"),
                                                ConstantValue.of(BIGINT, 42L),
                                                new AttributeKey(IR, REPEATABILITY),
                                                DETERMINISTIC,
                                                new AttributeKey(IR, SAFE),
                                                true,
                                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                                false))
                                        .build())
                                .build()));

        // an operation identical to the constant operation in the program
        MemoOperation presentMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect(TRINO)
                .withOperationId(new OperationId("constant", ImmutableList.of(), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withResultType(irType(BIGINT))
                .withChildren(ImmutableList.of())
                .withAttributes(attributes(
                        new AttributeKey(TRINO, "constant:value"),
                        ConstantValue.of(BIGINT, 42L),
                        new AttributeKey(IR, REPEATABILITY),
                        DETERMINISTIC,
                        new AttributeKey(IR, SAFE),
                        true,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS),
                        false))
                .withInherentOperationAttributeKeys(ImmutableSet.of(new AttributeKey(TRINO, "constant:value")))
                .build();

        // removing the only operation in the group fails
        assertThatThrownBy(() -> memo.removeOperation(presentMemoOperation))
                .hasMessage("Cannot remove the only operation from the group");

        // an operation not present in the program
        MemoOperation absentMemoOperation = MemoOperationBuilder.from(presentMemoOperation)
                .withAttributes(attributes(
                        new AttributeKey(TRINO, "constant:value"),
                        ConstantValue.of(BOOLEAN, true),
                        new AttributeKey(IR, REPEATABILITY),
                        DETERMINISTIC,
                        new AttributeKey(IR, SAFE),
                        true,
                        new AttributeKey(IR, HAS_SIDE_EFFECTS),
                        false))
                .build();

        // removing an operation not in the program fails
        assertThatThrownBy(() -> memo.removeOperation(absentMemoOperation))
                .hasMessageContaining("Removed operation not found in Memo");
    }

    @Test
    public void testPruneOrphanedGroups()
    {
        // two Values operations sharing one identical row.
        // The first Values operation is dead code, the second is the root.
        Values valuesOperation1 = valuesOfRows("1", ImmutableList.of(singleConstantRow("1", BIGINT, 42L), singleConstantRow("2", BIGINT, 0L)));
        Values valuesOperation2 = valuesOfRows("2", ImmutableList.of(singleConstantRow("3", BIGINT, 42L), singleConstantRow("4", BIGINT, 1L)));
        Program program = program(ImmutableList.of(valuesOperation1, valuesOperation2));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);
        memo.validateGroupToParentsMapping();

        // assert existing groups
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.<Integer, MemoGroupMatcher>builder()
                        .put(0, memoGroup().build()) // "%constant1"
                        .put(1, memoGroup() // "%row1"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(0))
                                        .build())
                                .build())
                        .put(2, memoGroup() // "%return1"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(1))
                                        .build())
                                .build())
                        .put(3, memoGroup().build()) // "%constant2"
                        .put(4, memoGroup() // "%row2"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(3))
                                        .build())
                                .build())
                        .put(5, memoGroup() // "%return2"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(4))
                                        .build())
                                .build())
                        .put(6, memoGroup() // "%values1"
                                .withOperations(memoOperation()
                                        .withChildren(
                                                groupChild(2),
                                                groupChild(5))
                                        .build())
                                .build())
                        .put(7, memoGroup().build()) // "%constant4"
                        .put(8, memoGroup() // "%row4"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(7))
                                        .build())
                                .build())
                        .put(9, memoGroup() // "%return4"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(8))
                                        .build())
                                .build())
                        .put(10, memoGroup() // "%values2"
                                .withOperations(memoOperation()
                                        .withChildren(
                                                groupChild(2),
                                                groupChild(9))
                                        .build())
                                .build())
                        .put(14, memoGroup() // "%output"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(10), groupChild(13))
                                        .build())
                                .build())
                        .put(15, memoGroup() // "%query"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(14))
                                        .build())
                                .build())
                        .buildOrThrow());

        // prune orphaned groups (unreachable from the root Query operation)
        // first Values operation is unreachable but its first row is reachable via the second Values operation
        memo.pruneOrphanedGroups();
        memo.validateGroupToParentsMapping();

        // assert removed groups
        assertMemoGroupsDoesNotContain(memo.groups(), 3, 4, 5, 6); // "%constant2", "%row2", "%return2", and "%values1"

        // assert remaining groups
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.<Integer, MemoGroupMatcher>builder()
                        .put(0, memoGroup().build()) // "%constant1"
                        .put(1, memoGroup() // "%row1"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(0))
                                        .build())
                                .build())
                        .put(2, memoGroup() // "%return1"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(1))
                                        .build())
                                .build())
                        .put(7, memoGroup().build()) // "%constant4"
                        .put(8, memoGroup() // "%row4"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(7))
                                        .build())
                                .build())
                        .put(9, memoGroup() // "%return4"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(8))
                                        .build())
                                .build())
                        .put(10, memoGroup() // "%values2"
                                .withOperations(memoOperation()
                                        .withChildren(
                                                groupChild(2),
                                                groupChild(9))
                                        .build())
                                .build())
                        .put(14, memoGroup() // "%output"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(10), groupChild(13))
                                        .build())
                                .build())
                        .put(15, memoGroup() // "%query"
                                .withOperations(memoOperation()
                                        .withChildren(groupChild(14))
                                        .build())
                                .build())
                        .buildOrThrow());
    }
}
