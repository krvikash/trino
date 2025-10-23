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
package io.trino.sql.planner.newirtoold;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.Context.RowField;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.ScalarProgramBuilder;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata.ComparisonOperator;
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation.Result;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestToOldIrScalarRewriter
{
    private static final Block.Parameter INPUT_ROW_PARAMETER = new Block.Parameter(
            "%input_row",
            irType(anonymousRow(BIGINT, BIGINT, BOOLEAN, VARCHAR)));

    private static final List<Symbol> INPUT_SYMBOLS = ImmutableList.of(
            new Symbol(BIGINT, "a"),
            new Symbol(BIGINT, "b"),
            new Symbol(BOOLEAN, "c"),
            new Symbol(VARCHAR, "d"));

    private static final Map<Symbol, RowField> SYMBOL_MAPPING = getSymbolMapping();

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testArray()
    {
        Array array = new Array(BIGINT, ImmutableList.of(new Reference(BIGINT, "b"), new Constant(BIGINT, 0L), new Reference(BIGINT, "a")));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L),
                        new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Array(
                                "%3",
                                BIGINT,
                                ImmutableList.of(new Result("%0", irType(BIGINT)), new Result("%1", irType(BIGINT)), new Result("%2", irType(BIGINT))),
                                emptySourceAttributes(3)),
                        new Return("%4", new Result("%3", irType(new ArrayType(BIGINT))), ImmutableMap.of())));
        assertRoundtrip(array, rewritten);
    }

    @Test
    public void testEmptyArray()
    {
        Array array = new Array(BIGINT, ImmutableList.of());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Array("%0", BIGINT, ImmutableList.of(), ImmutableList.of()),
                        new Return("%1", new Result("%0", irType(new ArrayType(BIGINT))), ImmutableMap.of())));
        assertRoundtrip(array, rewritten);
    }

    @Test
    public void testBetween()
    {
        Between between = new Between(new Reference(BIGINT, "b"), new Constant(BIGINT, 0L), new Reference(BIGINT, "a"));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L),
                        new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Between(
                                "%3",
                                new Result("%0", irType(BIGINT)),
                                new Result("%1", irType(BIGINT)),
                                new Result("%2", irType(BIGINT)),
                                emptySourceAttributes(3)),
                        new Return("%4", new Result("%3", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(between, rewritten);
    }

    @Test
    public void testBind()
    {
        Bind bind = new Bind(
                ImmutableList.of(new Reference(BIGINT, "b"), new Constant(BOOLEAN, true)),
                new Lambda(
                        ImmutableList.of(new Symbol(BIGINT, "lambda_parameter"), new Symbol(BOOLEAN, "lambda_parameter_0"), new Symbol(BIGINT, "lambda_parameter_1")),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "lambda_parameter"), new Reference(BIGINT, "lambda_parameter_1"))));
        Block.Parameter lambdaParameter = new Block.Parameter("%3", irType(anonymousRow(BIGINT, BOOLEAN, BIGINT)));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, true),
                        new io.trino.sql.dialect.trino.operation.Lambda(
                                "%2",
                                new Block(Optional.of("^lambda"),
                                        ImmutableList.of(lambdaParameter),
                                        ImmutableList.of(
                                                new FieldReference("%4", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new FieldReference("%5", lambdaParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Comparison(
                                                        "%6",
                                                        new Result("%4", irType(BIGINT)),
                                                        new Result("%5", irType(BIGINT)),
                                                        ComparisonOperator.GREATER_THAN,
                                                        emptySourceAttributes(2)),
                                                new Return("%7", new Result("%6", irType(BOOLEAN)), ImmutableMap.of())))),
                        new io.trino.sql.dialect.trino.operation.Bind(
                                "%8",
                                ImmutableList.of(new Result("%0", irType(BIGINT)), new Result("%1", irType(BOOLEAN))),
                                new Result("%2", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN, BIGINT), BOOLEAN))),
                                emptySourceAttributes(3)),
                        new Return("%9", new Result("%8", irType(new FunctionType(ImmutableList.of(BIGINT), BOOLEAN))), ImmutableMap.of())));
        assertRoundtrip(bind, rewritten);
    }

    @Test
    public void testBindWithoutArguments()
    {
        Bind bind = new Bind(
                ImmutableList.of(),
                new Lambda(
                        ImmutableList.of(new Symbol(BIGINT, "lambda_parameter"), new Symbol(BOOLEAN, "lambda_parameter_0"), new Symbol(BIGINT, "lambda_parameter_1")),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "lambda_parameter"), new Reference(BIGINT, "lambda_parameter_1"))));
        Block.Parameter lambdaParameter = new Block.Parameter("%1", irType(anonymousRow(BIGINT, BOOLEAN, BIGINT)));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Lambda(
                                "%0",
                                new Block(Optional.of("^lambda"),
                                        ImmutableList.of(lambdaParameter),
                                        ImmutableList.of(
                                                new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new FieldReference("%3", lambdaParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Comparison(
                                                        "%4",
                                                        new Result("%2", irType(BIGINT)),
                                                        new Result("%3", irType(BIGINT)),
                                                        ComparisonOperator.GREATER_THAN,
                                                        emptySourceAttributes(2)),
                                                new Return("%5", new Result("%4", irType(BOOLEAN)), ImmutableMap.of())))),
                        new io.trino.sql.dialect.trino.operation.Bind(
                                "%6",
                                ImmutableList.of(),
                                new Result("%0", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN, BIGINT), BOOLEAN))),
                                emptySourceAttributes(1)),
                        new Return("%7", new Result("%6", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN, BIGINT), BOOLEAN))), ImmutableMap.of())));
        assertRoundtrip(bind, rewritten);
    }

    @Test
    public void testCall()
    {
        ResolvedFunction addOperator = FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT));
        Call call = new Call(addOperator, ImmutableList.of(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Call(
                                "%2",
                                ImmutableList.of(new Result("%0", irType(BIGINT)), new Result("%1", irType(BIGINT))),
                                addOperator,
                                emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(call, rewritten);
    }

    @Test
    public void testCallWithoutArguments()
    {
        ResolvedFunction randomFunction = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());
        Call call = new Call(randomFunction, ImmutableList.of());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Call("%0", ImmutableList.of(), randomFunction, ImmutableList.of()),
                        new Return("%1", new Result("%0", irType(DOUBLE)), ImmutableMap.of())));
        assertRoundtrip(call, rewritten);
    }

    @Test
    public void testCase()
    {
        Case caseExpression = new Case(
                ImmutableList.of(
                        new WhenClause(new Reference(BOOLEAN, "c"), new Reference(BIGINT, "a")),
                        new WhenClause(new Constant(BOOLEAN, null), new Reference(BIGINT, "b"))),
                new Constant(BIGINT, 0L));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, null),
                        new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%4", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.Case(
                                "%5",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BOOLEAN))),
                                ImmutableList.of(new Result("%2", irType(BIGINT)), new Result("%3", irType(BIGINT))),
                                new Result("%4", irType(BIGINT)),
                                emptySourceAttributes(5)),
                        new Return("%6", new Result("%5", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(caseExpression, rewritten);
    }

    @Test
    public void testCast()
    {
        Cast cast = new Cast(new Reference(BIGINT, "b"), DOUBLE);
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Cast("%1", new Result("%0", irType(BIGINT)), DOUBLE, ImmutableMap.of()),
                        new Return("%2", new Result("%1", irType(DOUBLE)), ImmutableMap.of())));
        assertRoundtrip(cast, rewritten);
    }

    @Test
    public void testCoalesce()
    {
        Coalesce coalesce = new Coalesce(new Reference(BIGINT, "b"), new Reference(BIGINT, "a"), new Constant(BIGINT, 0L));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%2", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.Coalesce(
                                "%3",
                                ImmutableList.of(new Result("%0", irType(BIGINT)), new Result("%1", irType(BIGINT)), new Result("%2", irType(BIGINT))),
                                emptySourceAttributes(3)),
                        new Return("%4", new Result("%3", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(coalesce, rewritten);
    }

    @Test
    public void testComparison()
    {
        Comparison comparison = new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a"));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Comparison(
                                "%2",
                                new Result("%0", irType(BIGINT)),
                                new Result("%1", irType(BIGINT)),
                                ComparisonOperator.GREATER_THAN,
                                emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(comparison, rewritten);
    }

    @Test
    public void testConstant()
    {
        Constant constant = new Constant(BOOLEAN, true);
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(constant, rewritten);
    }

    @Test
    public void testFieldReferenceToSymbolReference()
    {
        // symbol reference of old IR is represented as FieldReference referencing the block parameter in new IR
        Reference reference = new Reference(BIGINT, "b");
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new Return("%1", new Result("%0", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(reference, rewritten);
    }

    @Test
    public void testFieldReferenceToFieldReference()
    {
        // row field reference of old IR is represented as FieldReference referencing a Value of row type in new IR
        io.trino.sql.ir.FieldReference fieldReference = new io.trino.sql.ir.FieldReference(new Row(ImmutableList.of(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))), 1);
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.Row(
                                "%2",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BIGINT))),
                                emptySourceAttributes(2)),
                        new FieldReference("%3", new Result("%2", irType(anonymousRow(BOOLEAN, BIGINT))), 1, ImmutableMap.of()),
                        new Return("%4", new Result("%3", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(fieldReference, rewritten);
    }

    @Test
    public void testIn()
    {
        In in = new In(new Reference(BIGINT, "b"), ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%2", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.In(
                                "%3",
                                new Result("%0", irType(BIGINT)),
                                ImmutableList.of(new Result("%1", irType(BIGINT)), new Result("%2", irType(BIGINT))),
                                emptySourceAttributes(3)),
                        new Return("%4", new Result("%3", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(in, rewritten);
    }

    @Test
    public void testInWithEmptyInList()
    {
        In in = new In(new Reference(BIGINT, "b"), ImmutableList.of());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.In("%1", new Result("%0", irType(BIGINT)), ImmutableList.of(), emptySourceAttributes(1)),
                        new Return("%2", new Result("%1", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(in, rewritten);
    }

    @Test
    public void testIsNull()
    {
        IsNull isNull = new IsNull(new Reference(BIGINT, "b"));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.IsNull("%1", new Result("%0", irType(BIGINT)), ImmutableMap.of()),
                        new Return("%2", new Result("%1", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(isNull, rewritten);
    }

    @Test
    public void testLambda()
    {
        // when new IR is rewritten to old IR, we drop all symbols. when rewriting back to old IR, we must reassign symbols.
        // for the reassigned symbols, we use default names: "lambda_parameter", "lambda_parameter_0", ...
        // for the purpose of roundtrip, we use those symbol names in the original expression.
        Lambda lambda = new Lambda(
                ImmutableList.of(new Symbol(BIGINT, "lambda_parameter"), new Symbol(BOOLEAN, "lambda_parameter_0")),
                new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "lambda_parameter"), new Reference(BIGINT, "b")),
                                new Reference(BOOLEAN, "lambda_parameter_0"))));
        Block.Parameter lambdaParameter = new Block.Parameter("%1", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Lambda(
                                "%0",
                                new Block(
                                        Optional.of("^lambda"),
                                        ImmutableList.of(lambdaParameter),
                                        ImmutableList.of(
                                                new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Comparison(
                                                        "%4",
                                                        new Result("%2", irType(BIGINT)),
                                                        new Result("%3", irType(BIGINT)),
                                                        ComparisonOperator.GREATER_THAN,
                                                        emptySourceAttributes(2)),
                                                new FieldReference("%5", lambdaParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Logical(
                                                        "%6",
                                                        ImmutableList.of(new Result("%4", irType(BOOLEAN)), new Result("%5", irType(BOOLEAN))),
                                                        LogicalOperator.AND,
                                                        emptySourceAttributes(2)),
                                                new Return("%7", new Result("%6", irType(BOOLEAN)), ImmutableMap.of())))),
                        new Return("%8", new Result("%0", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN), BOOLEAN))), ImmutableMap.of())));
        assertRoundtrip(lambda, rewritten);
    }

    @Test
    public void testLambdaWithoutArguments()
    {
        Lambda lambda = new Lambda(ImmutableList.of(), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")));
        Block.Parameter lambdaParameter = new Block.Parameter("%1", irType(EMPTY_ROW));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Lambda(
                                "%0",
                                new Block(
                                        Optional.of("^lambda"),
                                        ImmutableList.of(lambdaParameter),
                                        ImmutableList.of(
                                                new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Comparison(
                                                        "%4",
                                                        new Result("%2", irType(BIGINT)),
                                                        new Result("%3", irType(BIGINT)),
                                                        ComparisonOperator.GREATER_THAN,
                                                        emptySourceAttributes(2)),
                                                new Return("%5", new Result("%4", irType(BOOLEAN)), ImmutableMap.of())))),
                        new Return(
                                "%6",
                                new Result(
                                        "%0",
                                        irType(new FunctionType(ImmutableList.of(), BOOLEAN))),
                                ImmutableMap.of())));
        assertRoundtrip(lambda, rewritten);
    }

    @Test
    public void testNestedLambda()
    {
        // references from all levels of correlation:
        // (lambda_parameter, lambda_parameter_0) -> bind((lambda_parameter, c) to (lambda_parameter_1, lambda_parameter_2) -> lambda_parameter_1 > b AND lambda_parameter_0)
        Lambda lambda = new Lambda(
                ImmutableList.of(new Symbol(BIGINT, "lambda_parameter"), new Symbol(BOOLEAN, "lambda_parameter_0")),
                new Bind(
                        ImmutableList.of(new Reference(BIGINT, "lambda_parameter"), new Reference(BOOLEAN, "c")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "lambda_parameter_1"), new Symbol(BOOLEAN, "lambda_parameter_2")),
                                new Logical(
                                        AND,
                                        ImmutableList.of(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "lambda_parameter_1"), new Reference(BIGINT, "b")),
                                                new Reference(BOOLEAN, "lambda_parameter_0"))))));

        Block.Parameter outerLambdaParameter = new Block.Parameter("%1", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter innerLambdaParameter = new Block.Parameter("%5", irType(anonymousRow(BIGINT, BOOLEAN)));

        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Lambda(
                                "%0",
                                new Block(
                                        Optional.of("^lambda"),
                                        ImmutableList.of(outerLambdaParameter),
                                        ImmutableList.of(
                                                new FieldReference("%2", outerLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new FieldReference("%3", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                new io.trino.sql.dialect.trino.operation.Lambda(
                                                        "%4",
                                                        new Block(
                                                                Optional.of("^lambda"),
                                                                ImmutableList.of(innerLambdaParameter),
                                                                ImmutableList.of(
                                                                        new FieldReference("%6", innerLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                                        new FieldReference("%7", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                                        new io.trino.sql.dialect.trino.operation.Comparison(
                                                                                "%8",
                                                                                new Result("%6", irType(BIGINT)),
                                                                                new Result("%7", irType(BIGINT)),
                                                                                ComparisonOperator.GREATER_THAN,
                                                                                emptySourceAttributes(2)),
                                                                        new FieldReference("%9", outerLambdaParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                                                                        new io.trino.sql.dialect.trino.operation.Logical(
                                                                                "%10",
                                                                                ImmutableList.of(new Result("%8", irType(BOOLEAN)), new Result("%9", irType(BOOLEAN))),
                                                                                LogicalOperator.AND,
                                                                                emptySourceAttributes(2)),
                                                                        new Return("%11", new Result("%10", irType(BOOLEAN)), ImmutableMap.of())))),
                                                new io.trino.sql.dialect.trino.operation.Bind(
                                                        "%12",
                                                        ImmutableList.of(new Result("%2", irType(BIGINT)), new Result("%3", irType(BOOLEAN))),
                                                        new Result("%4", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN), BOOLEAN))),
                                                        emptySourceAttributes(3)),
                                                new Return("%13", new Result("%12", irType(new FunctionType(ImmutableList.of(), BOOLEAN))), ImmutableMap.of())))),
                        new Return("%14", new Result("%0", irType(new FunctionType(ImmutableList.of(BIGINT, BOOLEAN), new FunctionType(ImmutableList.of(), BOOLEAN)))), ImmutableMap.of())));
        assertRoundtrip(lambda, rewritten);
    }

    @Test
    public void testLogical()
    {
        Logical logical = new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "c"), new Constant(BOOLEAN, true), new Constant(BOOLEAN, null)));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, true),
                        new io.trino.sql.dialect.trino.operation.Constant("%2", BOOLEAN, null),
                        new io.trino.sql.dialect.trino.operation.Logical(
                                "%3",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BOOLEAN)), new Result("%2", irType(BOOLEAN))),
                                LogicalOperator.OR,
                                emptySourceAttributes(3)),
                        new Return("%4", new Result("%3", irType(BOOLEAN)), ImmutableMap.of())));
        assertRoundtrip(logical, rewritten);
    }

    @Test
    public void testNullIf()
    {
        NullIf nullIf = new NullIf(new Reference(BIGINT, "b"), new Reference(BIGINT, "a"));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.NullIf("%2", new Result("%0", irType(BIGINT)), new Result("%1", irType(BIGINT)), emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(nullIf, rewritten);
    }

    @Test
    public void testRow()
    {
        Row row = new Row(ImmutableList.of(new Reference(BOOLEAN, "c"), new Reference(BIGINT, "b")));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Row(
                                "%2",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BIGINT))),
                                emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(anonymousRow(BOOLEAN, BIGINT))), ImmutableMap.of())));
        assertRoundtrip(row, rewritten);
    }

    @Test
    public void testSwitch()
    {
        Switch switchExpression = new Switch(
                new Reference(BOOLEAN, "c"),
                ImmutableList.of(
                        new WhenClause(new Constant(BOOLEAN, true), new Reference(BIGINT, "a")),
                        new WhenClause(new Constant(BOOLEAN, false), new Reference(BIGINT, "b"))),
                new Constant(BIGINT, 0L));
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, true),
                        new io.trino.sql.dialect.trino.operation.Constant("%2", BOOLEAN, false),
                        new FieldReference("%3", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%4", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Constant("%5", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.Switch(
                                "%6",
                                new Result("%0", irType(BOOLEAN)),
                                ImmutableList.of(new Result("%1", irType(BOOLEAN)), new Result("%2", irType(BOOLEAN))),
                                ImmutableList.of(new Result("%3", irType(BIGINT)), new Result("%4", irType(BIGINT))),
                                new Result("%5", irType(BIGINT)),
                                emptySourceAttributes(6)),
                        new Return("%7", new Result("%6", irType(BIGINT)), ImmutableMap.of())));
        assertRoundtrip(switchExpression, rewritten);
    }

    @Test
    public void testMultipleBlockParameters()
    {
        Block.Parameter anotherParameter = new Block.Parameter("%anotherParameter", irType(anonymousRow(BOOLEAN, BIGINT, BIGINT)));
        List<Symbol> anotherSymbolList = ImmutableList.of(new Symbol(BOOLEAN, "x"), new Symbol(BIGINT, "y"), new Symbol(BIGINT, "z"));
        Map<Symbol, RowField> anotherSymbolMapping = ImmutableMap.of(
                anotherSymbolList.get(0), new RowField(anotherParameter, 0),
                anotherSymbolList.get(1), new RowField(anotherParameter, 1),
                anotherSymbolList.get(2), new RowField(anotherParameter, 2));

        Coalesce coalesce = new Coalesce(new Reference(BIGINT, "a"), new Reference(BIGINT, "y"), new Reference(BIGINT, "b"), new Reference(BIGINT, "z"));

        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER, anotherParameter),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", anotherParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%2", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%3", anotherParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Coalesce(
                                "%4",
                                ImmutableList.of(new Result("%0", irType(BIGINT)), new Result("%1", irType(BIGINT)), new Result("%2", irType(BIGINT)), new Result("%3", irType(BIGINT))),
                                emptySourceAttributes(4)),
                        new Return("%5", new Result("%4", irType(BIGINT)), ImmutableMap.of())));

        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ProgramBuilder.ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER, anotherParameter));
        coalesce.accept(scalarProgramBuilder, new Context(blockBuilder, ImmutableMap.<Symbol, RowField>builder().putAll(SYMBOL_MAPPING).putAll(anotherSymbolMapping).buildOrThrow()));
        scalarProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();
        assertThat(block).isEqualTo(rewritten);

        ToOldIrScalarRewriter scalarRewriter = new ToOldIrScalarRewriter(new SymbolAllocator());
        Expression roundtripExpression = scalarRewriter.toOldIr(block, ImmutableList.of(INPUT_SYMBOLS, anotherSymbolList));
        assertThat(roundtripExpression).isEqualTo(coalesce);
    }

    @Test
    public void testEmptyFieldSelector()
    {
        Block emptyFieldSelector = getEmptyFieldSelector(
                "^emptyFieldSelector",
                RowType.anonymous(INPUT_SYMBOLS.stream().map(Symbol::type).collect(toImmutableList())),
                new ProgramBuilder.ValueNameAllocator());
        Expression expression = new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(emptyFieldSelector, ImmutableList.of(INPUT_SYMBOLS));

        assertThat(expression).isEqualTo(new Constant(EMPTY_ROW, null));
    }

    @Test
    public void testGetSelectedSymbols()
    {
        Block fieldSelector = new Block(
                Optional.of("^fieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Row(
                                "%2",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BIGINT))),
                                emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(anonymousRow(BOOLEAN, BIGINT))), ImmutableMap.of())));

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getSelectedSymbols(fieldSelector, INPUT_SYMBOLS))
                .isEqualTo(ImmutableList.of(new Symbol(BOOLEAN, "c"), new Symbol(BIGINT, "a")));

        // empty field selector
        Block emptyFieldSelector = getEmptyFieldSelector(
                "^emptyFieldSelector",
                RowType.anonymous(INPUT_SYMBOLS.stream().map(Symbol::type).collect(toImmutableList())),
                new ProgramBuilder.ValueNameAllocator());

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getSelectedSymbols(emptyFieldSelector, INPUT_SYMBOLS))
                .isEqualTo(ImmutableList.of());

        // block is not a field selector
        Block notAFieldSelector = new Block(
                Optional.of("^notAFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getSelectedSymbols(notAFieldSelector, INPUT_SYMBOLS))
                .hasMessage("Expected field selector block");
    }

    @Test
    public void testGetOptionalSelectedSymbol()
    {
        // one symbol selected
        Block oneFieldSelector = new Block(
                Optional.of("^oneFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Row("%1", ImmutableList.of(new Result("%0", irType(BOOLEAN))), emptySourceAttributes(1)),
                        new Return("%2", new Result("%1", irType(anonymousRow(BOOLEAN))), ImmutableMap.of())));

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(oneFieldSelector, INPUT_SYMBOLS))
                .isEqualTo(Optional.of(new Symbol(BOOLEAN, "c")));

        // empty field selector
        Block emptyFieldSelector = getEmptyFieldSelector(
                "^emptyFieldSelector",
                RowType.anonymous(INPUT_SYMBOLS.stream().map(Symbol::type).collect(toImmutableList())),
                new ProgramBuilder.ValueNameAllocator());

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(emptyFieldSelector, INPUT_SYMBOLS))
                .isEqualTo(Optional.empty());

        // multiple symbols selected
        Block fieldSelector = new Block(
                Optional.of("^oneFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Row(
                                "%2",
                                ImmutableList.of(new Result("%0", irType(BOOLEAN)), new Result("%1", irType(BIGINT))),
                                emptySourceAttributes(2)),
                        new Return("%3", new Result("%2", irType(anonymousRow(BOOLEAN, BIGINT))), ImmutableMap.of())));
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(fieldSelector, INPUT_SYMBOLS))
                .hasMessage("expected one element but was: <c::[boolean], a::[bigint]>");

        // block is not a field selector
        Block notAFieldSelector = new Block(
                Optional.of("^notAFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(notAFieldSelector, INPUT_SYMBOLS))
                .hasMessage("Expected field selector block");
    }

    @Test
    public void testGetExpressions()
    {
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%1", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new FieldReference("%2", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new io.trino.sql.dialect.trino.operation.Comparison(
                                "%3",
                                new Result("%2", irType(BIGINT)),
                                new Result("%0", irType(BIGINT)),
                                ComparisonOperator.GREATER_THAN,
                                emptySourceAttributes(2)),
                        new io.trino.sql.dialect.trino.operation.Constant("%4", BIGINT, 0L),
                        new io.trino.sql.dialect.trino.operation.Row(
                                "%5",
                                ImmutableList.of(new Result("%4", irType(BIGINT)), new Result("%3", irType(BOOLEAN)), new Result("%1", irType(BOOLEAN))),
                                emptySourceAttributes(3)),
                        new Return("%6", new Result("%5", irType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN))), ImmutableMap.of())));

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getExpressions(block, INPUT_SYMBOLS))
                .isEqualTo(ImmutableList.of(
                        new Constant(BIGINT, 0L),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        new Reference(BOOLEAN, "c")));

        // block does not select expressions
        Block notARowOfExpressions = new Block(
                Optional.of("^notARowOfExpressions"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getExpressions(notARowOfExpressions, INPUT_SYMBOLS))
                .hasMessage("Expected block returning a row");
    }

    @Test
    public void testNoMappingForFieldReference()
    {
        // The rewrite can only resolve references to INPUT_ROW_PARAMETER. Fails on unknown parameter
        Block.Parameter unmappedParameter = new Block.Parameter("%unmapped", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block invalidReferenceBlock1 = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        new FieldReference("%0", unmappedParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES),
                        new Return("%1", new Result("%0", irType(BIGINT)), ImmutableMap.of())));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(invalidReferenceBlock1, ImmutableList.of(INPUT_SYMBOLS)))
                .hasMessage("Could not resolve reference %unmapped[0] as block parameter field");
    }

    @Test
    public void testSymbolListsMismatch()
    {
        Block.Parameter firstParameter = new Block.Parameter("%first", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter secondParameter = new Block.Parameter("%second", irType(EMPTY_ROW));

        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(firstParameter, secondParameter),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));

        // the symbols lists must match block parameters in size and types

        // too few symbol lists
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(
                block,
                ImmutableList.of(
                        // expecting 2 lists for 2 block parameters, got 1
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))))
                .hasMessage("Type mismatch");

        // too many symbol lists
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(
                block,
                ImmutableList.of(
                        // expecting 2 lists for 2 block parameters, got 3
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                        ImmutableList.of(),
                        ImmutableList.of(new Symbol(BOOLEAN, "c")))))
                .hasMessage("Type mismatch");

        // symbol list size mismatch
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(
                block,
                ImmutableList.of(
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                        // expecting empty list
                        ImmutableList.of(new Symbol(BOOLEAN, "c")))))
                .hasMessage("Type mismatch");

        // type mismatch
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(
                block,
                ImmutableList.of(
                        // expecting BIGINT, BOOLEAN
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b")),
                        ImmutableList.of())))
                .hasMessage("Type mismatch");

        // block parameter is not of relation row type: it is BIGINT
        Block.Parameter parameter = new Block.Parameter("%first", irType(BIGINT));
        Block bigintBlock = new Block(
                Optional.empty(),
                ImmutableList.of(parameter),
                ImmutableList.of(
                        new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true),
                        new Return("%1", new Result("%0", irType(BOOLEAN)), ImmutableMap.of())));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(
                bigintBlock,
                ImmutableList.of(ImmutableList.of(new Symbol(BIGINT, "a")))))
                .hasMessage("Type mismatch");
    }

    private void assertRoundtrip(Expression expression, Block rewritten)
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ProgramBuilder.ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));
        expression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING));
        scalarProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();
        assertThat(block).isEqualTo(rewritten);

        ToOldIrScalarRewriter scalarRewriter = new ToOldIrScalarRewriter(new SymbolAllocator());
        Expression roundtripExpression = scalarRewriter.toOldIr(block, ImmutableList.of(INPUT_SYMBOLS));
        assertThat(roundtripExpression).isEqualTo(expression);
    }

    private static Map<Symbol, RowField> getSymbolMapping()
    {
        return IntStream.range(0, INPUT_SYMBOLS.size())
                .boxed()
                .collect(toImmutableMap(INPUT_SYMBOLS::get, index -> new RowField(INPUT_ROW_PARAMETER, index)));
    }
}
