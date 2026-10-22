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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.ir.IrDialect.FunctionType;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage.identityRecursive;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;

public final class MemoTestingHelper
{
    public static final String TEST_DIALECT_NAME = "test_dialect";
    public static final AttributeKey TEST_DERIVED_ATTRIBUTE_KEY = new AttributeKey(TEST_DIALECT_NAME, "derived_attribute_key");
    public static final AttributeKey TEST_OPERATION_ATTRIBUTE_KEY = new AttributeKey(TEST_DIALECT_NAME, "operation_attribute_key");
    public static final Type TEST_TYPE = new Type(TEST_DIALECT_NAME, new Object());
    public static final String TEST_OPERATION_NAME = "test_operation";
    public static final int TEST_CHILD_GROUP_ID = 42;
    public static final Object TEST_OPERATION_ATTRIBUTE_OBJECT = new Object();
    public static final Object TEST_DERIVED_ATTRIBUTE_OBJECT = new Object();
    public static final MemoOperation TEST_MEMO_OPERATION = new MemoOperation(
            TEST_DIALECT_NAME,
            new Operation.OperationId(
                    TEST_OPERATION_NAME,
                    ImmutableList.of(TEST_TYPE),
                    ImmutableList.of(new Type(IR, new FunctionType(ImmutableList.of(TEST_TYPE, TEST_TYPE), TEST_TYPE)))),
            TEST_TYPE,
            ImmutableList.of(TEST_TYPE, TEST_TYPE),
            ImmutableList.of(
                    new ParameterChild(1),
                    new GroupChild(TEST_CHILD_GROUP_ID, identityRecursive(2, 0, 2))),
            attributes(
                    TEST_OPERATION_ATTRIBUTE_KEY,
                    TEST_OPERATION_ATTRIBUTE_OBJECT,
                    TEST_DERIVED_ATTRIBUTE_KEY,
                    TEST_DERIVED_ATTRIBUTE_OBJECT),
            ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY));

    private MemoTestingHelper() {}

    public static MemoChangesCollector ignoreChanges()
    {
        return new MemoChangesCollector();
    }

    public static RuleApplier ruleApplier(Memo memo)
    {
        OptimizationBudget budget = new OptimizationBudget(OptimizationLimits.defaultFor(memo.operationTokens(), 1));
        budget.consumeMemoGrowthTokens(memo.operationTokens());
        return new RuleApplier(memo, budget);
    }

    public static List<Operation> singleConstantRow(String suffix, io.trino.spi.type.Type type, Object value)
    {
        Constant constantOperation = new Constant("%constant" + suffix, type, value);
        Row rowOperation = new Row("%row" + suffix, ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation = new Return("%return" + suffix, rowOperation.result(), rowOperation.attributes());

        return ImmutableList.of(constantOperation, rowOperation, returnOperation);
    }

    public static Values valuesOfRows(List<List<Operation>> rows)
    {
        return valuesOfRows("", rows);
    }

    public static Values valuesOfRows(String suffix, List<List<Operation>> rows)
    {
        List<Block> blocks = rows.stream()
                .map(row -> new Block(Optional.of("^row"), ImmutableList.of(), row))
                .collect(toImmutableList());

        return new Values("%values" + suffix, (RowType) trinoType(blocks.getFirst().getReturnedType()), blocks);
    }

    public static Project identityProjection(Values values)
    {
        RowType rowType = (RowType) relationRowType(trinoType(values.result().type()));
        checkArgument(rowType.getFields().size() == 1, "expected single-column values: %s", rowType);

        Parameter assignmentsParameter = new Parameter("%assignments_parameter", irType(rowType));
        FieldReference fieldReferenceOperation = new FieldReference("%field_reference_assignments", assignmentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%row_assignments", ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%return_assignments", rowOperation.result(), rowOperation.attributes());
        return new Project(
                "%project",
                values.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(fieldReferenceOperation, rowOperation, returnOperation)),
                values.attributes());
    }

    public static Program program(Operation operation)
    {
        return program("_output", ImmutableList.of(operation));
    }

    public static Program program(List<Operation> operations)
    {
        return program("_output", operations);
    }

    public static Program program(String suffix, Operation operation)
    {
        return program(suffix, ImmutableList.of(operation));
    }

    /**
     * Creates a Program using given operations. The last operation is used as the root of the query.
     */
    private static Program program(String suffix, List<Operation> operations)
    {
        Parameter parameter = new Parameter("%parameter", irType(relationRowType(trinoType(operations.getLast().result().type()))));
        FieldReference fieldReferenceOperation = new FieldReference("%field_reference" + suffix, parameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%row" + suffix, ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%return" + suffix, rowOperation.result(), rowOperation.attributes());
        Output outputOperation = new Output(
                "%output",
                operations.getLast().result(),
                new Block(
                        Optional.of("^outputFieldSelector"),
                        ImmutableList.of(parameter),
                        ImmutableList.of(fieldReferenceOperation, rowOperation, returnOperation)),
                ImmutableList.of("output_column"),
                operations.getLast().attributes());

        Query queryOperation = new Query(
                "%query",
                new Block(
                        Optional.of("^query"),
                        ImmutableList.of(),
                        ImmutableList.<Operation>builder()
                                .addAll(operations)
                                .add(outputOperation)
                                .build()));

        return new Program(queryOperation);
    }

    public static Query query(String suffix, Operation operation)
    {
        Parameter parameter = new Parameter("%parameter", irType(relationRowType(trinoType(operation.result().type()))));
        FieldReference fieldReferenceOperation = new FieldReference("%field_reference", parameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%row", ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%return", rowOperation.result(), rowOperation.attributes());
        Output outputOperation = new Output(
                "%output",
                operation.result(),
                new Block(
                        Optional.of("^outputFieldSelector"),
                        ImmutableList.of(parameter),
                        ImmutableList.of(fieldReferenceOperation, rowOperation, returnOperation)),
                ImmutableList.of("output_column"),
                operation.attributes());

        return new Query(
                "%query" + suffix,
                new Block(
                        Optional.of("^query"),
                        ImmutableList.of(),
                        ImmutableList.of(operation, outputOperation)));
    }
}
