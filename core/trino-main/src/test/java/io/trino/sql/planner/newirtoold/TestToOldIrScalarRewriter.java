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
import io.trino.sql.ir.Match;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
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
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.equalityClause;
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

        FieldReference firstFieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L);
        FieldReference secondFieldReferenceOperation = new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Array arrayOperation = new io.trino.sql.dialect.trino.operation.Array(
                "%3",
                BIGINT,
                ImmutableList.of(firstFieldReferenceOperation.result(), constantOperation.result(), secondFieldReferenceOperation.result()),
                ImmutableList.of(firstFieldReferenceOperation.attributes(), constantOperation.attributes(), secondFieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%4", arrayOperation.result(), arrayOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        firstFieldReferenceOperation,
                        constantOperation,
                        secondFieldReferenceOperation,
                        arrayOperation,
                        returnOperation));

        assertRoundtrip(array, rewritten);
    }

    @Test
    public void testEmptyArray()
    {
        Array array = new Array(BIGINT, ImmutableList.of());

        io.trino.sql.dialect.trino.operation.Array arrayOperation = new io.trino.sql.dialect.trino.operation.Array("%0", BIGINT, ImmutableList.of(), ImmutableList.of());
        Return returnOperation = new Return("%1", arrayOperation.result(), arrayOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(arrayOperation, returnOperation));

        assertRoundtrip(array, rewritten);
    }

    @Test
    public void testBetween()
    {
        Between between = new Between(new Reference(BIGINT, "b"), new Constant(BIGINT, 0L), new Reference(BIGINT, "a"));

        FieldReference firstFieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L);
        FieldReference secondFieldReferenceOperation = new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Between betweenOperation = new io.trino.sql.dialect.trino.operation.Between(
                "%3",
                firstFieldReferenceOperation.result(),
                constantOperation.result(),
                secondFieldReferenceOperation.result(),
                ImmutableList.of(firstFieldReferenceOperation.attributes(), constantOperation.attributes(), secondFieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%4", betweenOperation.result(), betweenOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        firstFieldReferenceOperation,
                        constantOperation,
                        secondFieldReferenceOperation,
                        betweenOperation,
                        returnOperation));

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
        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, true);
        FieldReference fieldReferenceOperation2 = new FieldReference("%4", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%5", lambdaParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%6",
                fieldReferenceOperation2.result(),
                fieldReferenceOperation3.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation2.attributes(), fieldReferenceOperation3.attributes()));
        Return returnOperation1 = new Return("%7", comparisonOperation.result(), comparisonOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation = new io.trino.sql.dialect.trino.operation.Lambda(
                "%2",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation2,
                                fieldReferenceOperation3,
                                comparisonOperation,
                                returnOperation1)));
        io.trino.sql.dialect.trino.operation.Bind bindOperation = new io.trino.sql.dialect.trino.operation.Bind(
                "%8",
                ImmutableList.of(fieldReferenceOperation1.result(), constantOperation.result()),
                lambdaOperation.result(),
                ImmutableList.of(fieldReferenceOperation1.attributes(), constantOperation.attributes(), lambdaOperation.attributes()));
        Return eturnOperation2 = new Return("%9", bindOperation.result(), bindOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        constantOperation,
                        lambdaOperation,
                        bindOperation,
                        eturnOperation2));

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
        FieldReference fieldReferenceOperation1 = new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%3", lambdaParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%4",
                fieldReferenceOperation1.result(),
                fieldReferenceOperation2.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation1 = new Return("%5", comparisonOperation.result(), comparisonOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation = new io.trino.sql.dialect.trino.operation.Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation1,
                                fieldReferenceOperation2,
                                comparisonOperation,
                                returnOperation1)));
        io.trino.sql.dialect.trino.operation.Bind bindOperation = new io.trino.sql.dialect.trino.operation.Bind(
                "%6",
                ImmutableList.of(),
                lambdaOperation.result(),
                ImmutableList.of(lambdaOperation.attributes()));
        Return returnOperation2 = new Return("%7", bindOperation.result(), bindOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        lambdaOperation,
                        bindOperation,
                        returnOperation2));

        assertRoundtrip(bind, rewritten);
    }

    @Test
    public void testCall()
    {
        ResolvedFunction addOperator = FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT));
        Call call = new Call(addOperator, ImmutableList.of(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Call callOperation = new io.trino.sql.dialect.trino.operation.Call(
                "%2",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result()),
                addOperator,
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation = new Return("%3", callOperation.result(), callOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        callOperation,
                        returnOperation));

        assertRoundtrip(call, rewritten);
    }

    @Test
    public void testCallWithoutArguments()
    {
        ResolvedFunction randomFunction = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());
        Call call = new Call(randomFunction, ImmutableList.of());

        io.trino.sql.dialect.trino.operation.Call callOperation = new io.trino.sql.dialect.trino.operation.Call("%0", ImmutableList.of(), randomFunction, ImmutableList.of());
        Return returnOperation = new Return("%1", callOperation.result(), callOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        callOperation,
                        returnOperation));

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

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation1 = new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, null);
        FieldReference fieldReferenceOperation2 = new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation2 = new io.trino.sql.dialect.trino.operation.Constant("%4", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.Case caseOperation = new io.trino.sql.dialect.trino.operation.Case(
                "%5",
                ImmutableList.of(fieldReferenceOperation1.result(), constantOperation1.result()),
                ImmutableList.of(fieldReferenceOperation2.result(), fieldReferenceOperation3.result()),
                constantOperation2.result(),
                ImmutableList.of(fieldReferenceOperation1.attributes(), constantOperation1.attributes(), fieldReferenceOperation2.attributes(), fieldReferenceOperation3.attributes(), constantOperation2.attributes()));
        Return returnOperation = new Return("%6", caseOperation.result(), caseOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        constantOperation1,
                        fieldReferenceOperation2,
                        fieldReferenceOperation3,
                        constantOperation2,
                        caseOperation,
                        returnOperation));

        assertRoundtrip(caseExpression, rewritten);
    }

    @Test
    public void testCast()
    {
        Cast cast = new Cast(new Reference(BIGINT, "b"), DOUBLE);

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Cast castOperation = new io.trino.sql.dialect.trino.operation.Cast("%1", fieldReferenceOperation.result(), DOUBLE, fieldReferenceOperation.attributes());
        Return returnOperation = new Return("%2", castOperation.result(), castOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        castOperation,
                        returnOperation));

        assertRoundtrip(cast, rewritten);
    }

    @Test
    public void testCoalesce()
    {
        Coalesce coalesce = new Coalesce(new Reference(BIGINT, "b"), new Reference(BIGINT, "a"), new Constant(BIGINT, 0L));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%2", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.Coalesce coalesceOperation = new io.trino.sql.dialect.trino.operation.Coalesce(
                "%3",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result(), constantOperation.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%4", coalesceOperation.result(), coalesceOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        constantOperation,
                        coalesceOperation,
                        returnOperation));

        assertRoundtrip(coalesce, rewritten);
    }

    @Test
    public void testComparison()
    {
        Comparison comparison = new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a"));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%2",
                fieldReferenceOperation1.result(),
                fieldReferenceOperation2.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation = new Return("%3", comparisonOperation.result(), comparisonOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        comparisonOperation,
                        returnOperation));

        assertRoundtrip(comparison, rewritten);
    }

    @Test
    public void testConstant()
    {
        Constant constant = new Constant(BOOLEAN, true);

        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        constantOperation,
                        returnOperation));

        assertRoundtrip(constant, rewritten);
    }

    @Test
    public void testFieldReferenceToSymbolReference()
    {
        // symbol reference of old IR is represented as FieldReference referencing the block parameter in new IR
        Reference reference = new Reference(BIGINT, "b");

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        returnOperation));

        assertRoundtrip(reference, rewritten);
    }

    @Test
    public void testFieldReferenceToFieldReference()
    {
        // row field reference of old IR is represented as FieldReference referencing a Value of row type in new IR
        io.trino.sql.ir.FieldReference fieldReference = new io.trino.sql.ir.FieldReference(new Row(ImmutableList.of(new Constant(BOOLEAN, true), new Constant(BIGINT, 0L))), 1);

        io.trino.sql.dialect.trino.operation.Constant constantOperation1 = new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true);
        io.trino.sql.dialect.trino.operation.Constant constantOperation2 = new io.trino.sql.dialect.trino.operation.Constant("%1", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.Row rowOperation = new io.trino.sql.dialect.trino.operation.Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        FieldReference fieldReferenceOperation = new FieldReference("%3", rowOperation.result(), 1, rowOperation.attributes());
        Return returnOperation = new Return("%4", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        rowOperation,
                        fieldReferenceOperation,
                        returnOperation));

        assertRoundtrip(fieldReference, rewritten);
    }

    @Test
    public void testIn()
    {
        In in = new In(new Reference(BIGINT, "b"), ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%2", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.In inOperation = new io.trino.sql.dialect.trino.operation.In(
                "%3",
                fieldReferenceOperation1.result(),
                ImmutableList.of(fieldReferenceOperation2.result(), constantOperation.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%4", inOperation.result(), inOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        constantOperation,
                        inOperation,
                        returnOperation));

        assertRoundtrip(in, rewritten);
    }

    @Test
    public void testInWithEmptyInList()
    {
        In in = new In(new Reference(BIGINT, "b"), ImmutableList.of());

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.In inOperation = new io.trino.sql.dialect.trino.operation.In(
                "%1",
                fieldReferenceOperation.result(),
                ImmutableList.of(),
                ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%2", inOperation.result(), inOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        inOperation,
                        returnOperation));

        assertRoundtrip(in, rewritten);
    }

    @Test
    public void testIsNull()
    {
        IsNull isNull = new IsNull(new Reference(BIGINT, "b"));

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.IsNull isNullOperation = new io.trino.sql.dialect.trino.operation.IsNull(
                "%1",
                fieldReferenceOperation.result(),
                fieldReferenceOperation.attributes());
        Return returnOperation = new Return("%2", isNullOperation.result(), isNullOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        isNullOperation,
                        returnOperation));

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
        FieldReference fieldReferenceOperation1 = new FieldReference("%2", lambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%4",
                fieldReferenceOperation1.result(),
                fieldReferenceOperation2.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        FieldReference fieldReferenceOperation3 = new FieldReference("%5", lambdaParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Logical logicalOperation = new io.trino.sql.dialect.trino.operation.Logical(
                "%6",
                ImmutableList.of(comparisonOperation.result(), fieldReferenceOperation3.result()),
                LogicalOperator.AND,
                ImmutableList.of(comparisonOperation.attributes(), fieldReferenceOperation3.attributes()));
        Return returnOperation1 = new Return("%7", logicalOperation.result(), logicalOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation = new io.trino.sql.dialect.trino.operation.Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation1,
                                fieldReferenceOperation2,
                                comparisonOperation,
                                fieldReferenceOperation3,
                                logicalOperation,
                                returnOperation1)));
        Return returnOperation = new Return("%8", lambdaOperation.result(), lambdaOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        lambdaOperation,
                        returnOperation));

        assertRoundtrip(lambda, rewritten);
    }

    @Test
    public void testLambdaWithoutArguments()
    {
        Lambda lambda = new Lambda(ImmutableList.of(), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")));

        Block.Parameter lambdaParameter = new Block.Parameter("%1", irType(EMPTY_ROW));
        FieldReference fieldReferenceOperation1 = new FieldReference("%2", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%3", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%4",
                fieldReferenceOperation1.result(),
                fieldReferenceOperation2.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation1 = new Return("%5", comparisonOperation.result(), comparisonOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation = new io.trino.sql.dialect.trino.operation.Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation1,
                                fieldReferenceOperation2,
                                comparisonOperation,
                                returnOperation1)));
        Return returnOperation = new Return("%6", lambdaOperation.result(), lambdaOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        lambdaOperation,
                        returnOperation));

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

        FieldReference fieldReferenceOperation1 = new FieldReference("%2", outerLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%3", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%6", innerLambdaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation4 = new FieldReference("%7", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation = new io.trino.sql.dialect.trino.operation.Comparison(
                "%8",
                fieldReferenceOperation3.result(),
                fieldReferenceOperation4.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation3.attributes(), fieldReferenceOperation4.attributes()));
        FieldReference fieldReferenceOperation5 = new FieldReference("%9", outerLambdaParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Logical logicalOperation = new io.trino.sql.dialect.trino.operation.Logical(
                "%10",
                ImmutableList.of(comparisonOperation.result(), fieldReferenceOperation5.result()),
                LogicalOperator.AND,
                ImmutableList.of(comparisonOperation.attributes(), fieldReferenceOperation5.attributes()));
        Return returnOperation1 = new Return("%11", logicalOperation.result(), logicalOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation1 = new io.trino.sql.dialect.trino.operation.Lambda(
                "%4",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(innerLambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation3,
                                fieldReferenceOperation4,
                                comparisonOperation,
                                fieldReferenceOperation5,
                                logicalOperation,
                                returnOperation1)));
        io.trino.sql.dialect.trino.operation.Bind bindOperation = new io.trino.sql.dialect.trino.operation.Bind(
                "%12",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result()),
                lambdaOperation1.result(),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes(), lambdaOperation1.attributes()));
        Return returnOperation2 = new Return("%13", bindOperation.result(), bindOperation.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation2 = new io.trino.sql.dialect.trino.operation.Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(outerLambdaParameter),
                        ImmutableList.of(
                                fieldReferenceOperation1,
                                fieldReferenceOperation2,
                                lambdaOperation1,
                                bindOperation,
                                returnOperation2)));
        Return returnOperation3 = new Return("%14", lambdaOperation2.result(), lambdaOperation2.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        lambdaOperation2,
                        returnOperation3));

        assertRoundtrip(lambda, rewritten);
    }

    @Test
    public void testLogical()
    {
        Logical logical = new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "c"), new Constant(BOOLEAN, true), new Constant(BOOLEAN, null)));

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation1 = new io.trino.sql.dialect.trino.operation.Constant("%1", BOOLEAN, true);
        io.trino.sql.dialect.trino.operation.Constant constantOperation2 = new io.trino.sql.dialect.trino.operation.Constant("%2", BOOLEAN, null);
        io.trino.sql.dialect.trino.operation.Logical logicalOperation = new io.trino.sql.dialect.trino.operation.Logical(
                "%3",
                ImmutableList.of(fieldReferenceOperation.result(), constantOperation1.result(), constantOperation2.result()),
                LogicalOperator.OR,
                ImmutableList.of(fieldReferenceOperation.attributes(), constantOperation1.attributes(), constantOperation2.attributes()));
        Return returnOperation = new Return("%4", logicalOperation.result(), logicalOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        constantOperation1,
                        constantOperation2,
                        logicalOperation,
                        returnOperation));

        assertRoundtrip(logical, rewritten);
    }

    @Test
    public void testNullIf()
    {
        NullIf nullIf = new NullIf(new Reference(BIGINT, "b"), new Reference(BIGINT, "a"));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.NullIf nullIfOperation = new io.trino.sql.dialect.trino.operation.NullIf(
                "%2",
                fieldReferenceOperation1.result(),
                fieldReferenceOperation2.result(),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation = new Return("%3", nullIfOperation.result(), nullIfOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        nullIfOperation,
                        returnOperation));

        assertRoundtrip(nullIf, rewritten);
    }

    @Test
    public void testRow()
    {
        Row row = new Row(ImmutableList.of(new Reference(BOOLEAN, "c"), new Reference(BIGINT, "b")));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Row rowOperation = new io.trino.sql.dialect.trino.operation.Row(
                "%2",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation = new Return("%3", rowOperation.result(), rowOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        rowOperation,
                        returnOperation));

        assertRoundtrip(row, rewritten);
    }

    @Test
    public void testMatch()
    {
        Match matchExpression = new Match(
                new Reference(BOOLEAN, "c"),
                ImmutableList.of(
                        equalityClause(new Symbol(BOOLEAN, "lambda_parameter"), new Constant(BOOLEAN, true), new Reference(BIGINT, "a")),
                        equalityClause(new Symbol(BOOLEAN, "lambda_parameter_0"), new Constant(BOOLEAN, false), new Reference(BIGINT, "b"))),
                new Constant(BIGINT, 0L));

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

        Block.Parameter lambdaArgument1 = new Block.Parameter("%2", irType(anonymousRow(BOOLEAN)));
        FieldReference lamdaFieldReference1 = new FieldReference("%3", lambdaArgument1, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation1 = new io.trino.sql.dialect.trino.operation.Constant("%4", BOOLEAN, true);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation1 = new io.trino.sql.dialect.trino.operation.Comparison(
                "%5",
                lamdaFieldReference1.result(),
                constantOperation1.result(),
                ComparisonOperator.EQUAL,
                ImmutableList.of(lamdaFieldReference1.attributes(), constantOperation1.attributes()));

        Return returnOperation1 = new Return("%6", comparisonOperation1.result(), comparisonOperation1.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation1 = new io.trino.sql.dialect.trino.operation.Lambda(
                "%1",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument1),
                        ImmutableList.of(
                                lamdaFieldReference1,
                                constantOperation1,
                                comparisonOperation1,
                                returnOperation1)));

        Block.Parameter lambdaArgument2 = new Block.Parameter("%8", irType(anonymousRow(BOOLEAN)));
        FieldReference lamdaFieldReference2 = new FieldReference("%9", lambdaArgument2, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation2 = new io.trino.sql.dialect.trino.operation.Constant("%10", BOOLEAN, false);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation2 = new io.trino.sql.dialect.trino.operation.Comparison(
                "%11",
                lamdaFieldReference2.result(),
                constantOperation2.result(),
                ComparisonOperator.EQUAL,
                ImmutableList.of(lamdaFieldReference2.attributes(), constantOperation2.attributes()));
        Return returnOperation2 = new Return("%12", comparisonOperation2.result(), comparisonOperation2.attributes());
        io.trino.sql.dialect.trino.operation.Lambda lambdaOperation2 = new io.trino.sql.dialect.trino.operation.Lambda(
                "%7",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument2),
                        ImmutableList.of(
                                lamdaFieldReference2,
                                constantOperation2,
                                comparisonOperation2,
                                returnOperation2)));
        FieldReference fieldReferenceOperation2 = new FieldReference("%13", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%14", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Constant constantOperation3 = new io.trino.sql.dialect.trino.operation.Constant("%15", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.Match matchOperation = new io.trino.sql.dialect.trino.operation.Match(
                "%16",
                fieldReferenceOperation1.result(),
                ImmutableList.of(lambdaOperation1.result(), lambdaOperation2.result()),
                ImmutableList.of(fieldReferenceOperation2.result(), fieldReferenceOperation3.result()),
                constantOperation3.result(),
                ImmutableList.of(
                        fieldReferenceOperation1.attributes(),
                        lambdaOperation1.attributes(),
                        lambdaOperation2.attributes(),
                        fieldReferenceOperation2.attributes(),
                        fieldReferenceOperation3.attributes(),
                        constantOperation3.attributes()));
        Return returnOperation = new Return("%17", matchOperation.result(), matchOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        lambdaOperation1,
                        lambdaOperation2,
                        fieldReferenceOperation2,
                        fieldReferenceOperation3,
                        constantOperation3,
                        matchOperation,
                        returnOperation));

        assertRoundtrip(matchExpression, rewritten);
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

        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", anotherParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%2", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation4 = new FieldReference("%3", anotherParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Coalesce coalesceOperation = new io.trino.sql.dialect.trino.operation.Coalesce(
                "%4",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result(), fieldReferenceOperation3.result(), fieldReferenceOperation4.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes(), fieldReferenceOperation3.attributes(), fieldReferenceOperation4.attributes()));
        Return returnOperation = new Return("%5", coalesceOperation.result(), coalesceOperation.attributes());
        Block rewritten = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER, anotherParameter),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        fieldReferenceOperation3,
                        fieldReferenceOperation4,
                        coalesceOperation,
                        returnOperation));

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
        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Row rowOperation = new io.trino.sql.dialect.trino.operation.Row(
                "%2",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation1 = new Return("%3", rowOperation.result(), rowOperation.attributes());
        Block fieldSelector = new Block(
                Optional.of("^fieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        rowOperation,
                        returnOperation1));

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
        Return returnOperation2 = new Return("%1", fieldReferenceOperation1.result(), fieldReferenceOperation1.attributes());
        Block notAFieldSelector = new Block(
                Optional.of("^notAFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        returnOperation2));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getSelectedSymbols(notAFieldSelector, INPUT_SYMBOLS))
                .hasMessage("Expected field selector block");
    }

    @Test
    public void testGetOptionalSelectedSymbol()
    {
        // one symbol selected
        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Row rowOperation1 = new io.trino.sql.dialect.trino.operation.Row(
                "%1",
                ImmutableList.of(fieldReferenceOperation1.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes()));
        Return returnOperation1 = new Return("%2", rowOperation1.result(), rowOperation1.attributes());
        Block oneFieldSelector = new Block(
                Optional.of("^oneFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        rowOperation1,
                        returnOperation1));

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
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Row rowOperation2 = new io.trino.sql.dialect.trino.operation.Row(
                "%2",
                ImmutableList.of(fieldReferenceOperation1.result(), fieldReferenceOperation2.result()),
                ImmutableList.of(fieldReferenceOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation2 = new Return("%3", rowOperation2.result(), rowOperation2.attributes());
        Block fieldSelector = new Block(
                Optional.of("^oneFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        rowOperation2,
                        returnOperation2));
        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(fieldSelector, INPUT_SYMBOLS))
                .hasMessage("expected one element but was: <c::[boolean], a::[bigint]>");

        // block is not a field selector
        Return returnOperation3 = new Return("%1", fieldReferenceOperation1.result(), fieldReferenceOperation1.attributes());
        Block notAFieldSelector = new Block(
                Optional.of("^notAFieldSelector"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        returnOperation3));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getOptionalSelectedSymbol(notAFieldSelector, INPUT_SYMBOLS))
                .hasMessage("Expected field selector block");
    }

    @Test
    public void testGetExpressions()
    {
        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation2 = new FieldReference("%1", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperation3 = new FieldReference("%2", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        io.trino.sql.dialect.trino.operation.Comparison comparisonOperation1 = new io.trino.sql.dialect.trino.operation.Comparison(
                "%3",
                fieldReferenceOperation3.result(),
                fieldReferenceOperation1.result(),
                ComparisonOperator.GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation3.attributes(), fieldReferenceOperation1.attributes()));
        io.trino.sql.dialect.trino.operation.Constant constantOperation1 = new io.trino.sql.dialect.trino.operation.Constant("%4", BIGINT, 0L);
        io.trino.sql.dialect.trino.operation.Row rowOperation = new io.trino.sql.dialect.trino.operation.Row(
                "%5",
                ImmutableList.of(constantOperation1.result(), comparisonOperation1.result(), fieldReferenceOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), comparisonOperation1.attributes(), fieldReferenceOperation2.attributes()));
        Return returnOperation1 = new Return("%6", rowOperation.result(), rowOperation.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation1,
                        fieldReferenceOperation2,
                        fieldReferenceOperation3,
                        comparisonOperation1,
                        constantOperation1,
                        rowOperation,
                        returnOperation1));

        assertThat(new ToOldIrScalarRewriter(new SymbolAllocator()).getExpressions(block, INPUT_SYMBOLS))
                .isEqualTo(ImmutableList.of(
                        new Constant(BIGINT, 0L),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        new Reference(BOOLEAN, "c")));

        // block does not select expressions
        FieldReference fieldReferenceOperation4 = new FieldReference("%0", INPUT_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation2 = new Return("%1", fieldReferenceOperation4.result(), fieldReferenceOperation4.attributes());
        Block notARowOfExpressions = new Block(
                Optional.of("^notARowOfExpressions"),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation4,
                        returnOperation2));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).getExpressions(notARowOfExpressions, INPUT_SYMBOLS))
                .hasMessage("Expected block returning a row");
    }

    @Test
    public void testNoMappingForFieldReference()
    {
        // The rewrite can only resolve references to INPUT_ROW_PARAMETER. Fails on unknown parameter
        Block.Parameter unmappedParameter = new Block.Parameter("%unmapped", irType(anonymousRow(BIGINT, BOOLEAN)));
        FieldReference fieldReferenceOperation = new FieldReference("%0", unmappedParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Block invalidReferenceBlock1 = new Block(
                Optional.empty(),
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(
                        fieldReferenceOperation,
                        returnOperation));

        assertThatThrownBy(() -> new ToOldIrScalarRewriter(new SymbolAllocator()).toOldIr(invalidReferenceBlock1, ImmutableList.of(INPUT_SYMBOLS)))
                .hasMessage("Could not resolve reference %unmapped[0] as block parameter field");
    }

    @Test
    public void testSymbolListsMismatch()
    {
        Block.Parameter firstParameter = new Block.Parameter("%first", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter secondParameter = new Block.Parameter("%second", irType(EMPTY_ROW));

        io.trino.sql.dialect.trino.operation.Constant constantOperation = new io.trino.sql.dialect.trino.operation.Constant("%0", BOOLEAN, true);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());
        Block block = new Block(
                Optional.empty(),
                ImmutableList.of(firstParameter, secondParameter),
                ImmutableList.of(
                        constantOperation,
                        returnOperation));

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
                        constantOperation,
                        returnOperation));

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
