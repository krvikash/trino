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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Context.RowField;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Bind;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Case;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operation.Coalesce;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Let;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Match;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.CastKind;
import io.trino.sql.ir.Cast.Kind;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical.Operator;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.OR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestScalarProgramBuilder
{
    private static final Block.Parameter INPUT_ROW_PARAMETER = new Block.Parameter(
            "%input_row",
            irType(anonymousRow(BIGINT, BOOLEAN)));

    private static final Map<Symbol, RowField> SYMBOL_MAPPING = ImmutableMap.of(
            new Symbol(BIGINT, "a"), new RowField(INPUT_ROW_PARAMETER, 0),
            new Symbol(BOOLEAN, "b"), new RowField(INPUT_ROW_PARAMETER, 1));

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final ResolvedFunction LESS_THAN_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(BIGINT, BIGINT));

    private static final ResolvedFunction LESS_THAN_OR_EQUAL_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(BIGINT, BIGINT));

    private static final ResolvedFunction EQUAL_BIGINT = FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testArray()
    {
        io.trino.sql.ir.Array arrayExpression = new io.trino.sql.ir.Array(
                BOOLEAN,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, false)));

        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Array arrayOperation = new Array(
                "%3",
                BOOLEAN,
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                arrayExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        arrayOperation),
                new ArrayType(BOOLEAN));
    }

    @Test
    public void testBetween()
    {
        // the desugared form of `0 BETWEEN 1 AND 2` with a trivial value (see IrExpressions.between).
        // It maps 1-1 to new IR operations: there is no dedicated between operation.
        Expression betweenExpression = new io.trino.sql.ir.Logical(
                Operator.AND,
                ImmutableList.of(
                        new io.trino.sql.ir.Call(LESS_THAN_OR_EQUAL_BIGINT, ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 1L), new io.trino.sql.ir.Constant(BIGINT, 0L))),
                        new io.trino.sql.ir.Call(LESS_THAN_OR_EQUAL_BIGINT, ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 0L), new io.trino.sql.ir.Constant(BIGINT, 2L)))));

        Constant constantOperationMin = new Constant("%0", BIGINT, 1L);
        Constant constantOperationValue1 = new Constant("%1", BIGINT, 0L);
        Call callOperation1 = new Call(
                "%2",
                ImmutableList.of(constantOperationMin.result(), constantOperationValue1.result()),
                LESS_THAN_OR_EQUAL_BIGINT,
                ImmutableList.of(constantOperationMin.attributes(), constantOperationValue1.attributes()));
        Constant constantOperationValue2 = new Constant("%3", BIGINT, 0L);
        Constant constantOperationMax = new Constant("%4", BIGINT, 2L);
        Call callOperation2 = new Call(
                "%5",
                ImmutableList.of(constantOperationValue2.result(), constantOperationMax.result()),
                LESS_THAN_OR_EQUAL_BIGINT,
                ImmutableList.of(constantOperationValue2.attributes(), constantOperationMax.attributes()));
        Logical logicalOperation = new Logical(
                "%6",
                ImmutableList.of(callOperation1.result(), callOperation2.result()),
                AND,
                ImmutableList.of(callOperation1.attributes(), callOperation2.attributes()));

        assertProgram(
                betweenExpression,
                ImmutableList.of(
                        constantOperationMin,
                        constantOperationValue1,
                        callOperation1,
                        constantOperationValue2,
                        constantOperationMax,
                        callOperation2,
                        logicalOperation),
                BOOLEAN);
    }

    @Test
    public void testBind()
    {
        io.trino.sql.ir.Bind bindExpression = new io.trino.sql.ir.Bind(
                ImmutableList.of(new Reference(BIGINT, "a")),
                new io.trino.sql.ir.Lambda(
                        ImmutableList.of(new Symbol(BIGINT, "x")),
                        new io.trino.sql.ir.Call(
                                LESS_THAN_BIGINT,
                                ImmutableList.of(new Reference(BIGINT, "x"), new io.trino.sql.ir.Constant(BIGINT, 0L)))));

        FieldReference fieldReferenceOperationA = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Block.Parameter lambdaArgument = new Block.Parameter("%2", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%3", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%4", BIGINT, 0L);
        Call comparisonOperation = new Call(
                "%5",
                ImmutableList.of(fieldReferenceOperationX.result(), constantOperation.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(fieldReferenceOperationX.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%6", comparisonOperation.result(), comparisonOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%1",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                constantOperation,
                                comparisonOperation,
                                returnOperation)));
        Bind bindOperation = new Bind(
                "%7",
                ImmutableList.of(fieldReferenceOperationA.result()),
                lambdaOperation.result(),
                ImmutableList.of(fieldReferenceOperationA.attributes(), lambdaOperation.attributes()));

        assertProgram(
                bindExpression,
                ImmutableList.of(
                        fieldReferenceOperationA,
                        lambdaOperation,
                        bindOperation),
                new FunctionType(ImmutableList.of(), BOOLEAN));
    }

    @Test
    public void testCallWithoutArguments()
    {
        ResolvedFunction randomFunction = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());

        io.trino.sql.ir.Call callExpression = new io.trino.sql.ir.Call(randomFunction, ImmutableList.of());

        Call callOperation = new Call("%0", ImmutableList.of(), randomFunction, ImmutableList.of());

        assertProgram(callExpression, ImmutableList.of(callOperation), DOUBLE);
    }

    @Test
    public void testCallWithArguments()
    {
        ResolvedFunction addOperator = FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

        io.trino.sql.ir.Call callExpression = new io.trino.sql.ir.Call(
                addOperator,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 1L),
                        new io.trino.sql.ir.Constant(BIGINT, 2L)));

        Constant constantOperation1 = new Constant("%0", BIGINT, 1L);
        Constant constantOperation2 = new Constant("%1", BIGINT, 2L);
        Call callOperation = new Call(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                addOperator,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        assertProgram(
                callExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        callOperation),
                BIGINT);
    }

    @Test
    public void testCase()
    {
        io.trino.sql.ir.Case caseExpression = new io.trino.sql.ir.Case(
                ImmutableList.of(
                        new WhenClause(new io.trino.sql.ir.Constant(BOOLEAN, true), new io.trino.sql.ir.Constant(BIGINT, 0L)),
                        new WhenClause(new io.trino.sql.ir.Constant(BOOLEAN, false), new io.trino.sql.ir.Constant(BIGINT, 1L))),
                new io.trino.sql.ir.Constant(BIGINT, 2L));

        Constant constantOperationWhen1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperationWhen2 = new Constant("%1", BOOLEAN, false);
        Constant constantOperationThen1 = new Constant("%2", BIGINT, 0L);
        Constant constantOperationThen2 = new Constant("%3", BIGINT, 1L);
        Constant constantOperationDefault = new Constant("%4", BIGINT, 2L);
        Case caseOperation = new Case(
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationWhen1.attributes(),
                        constantOperationWhen2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        assertProgram(
                caseExpression,
                ImmutableList.of(
                        constantOperationWhen1,
                        constantOperationWhen2,
                        constantOperationThen1,
                        constantOperationThen2,
                        constantOperationDefault,
                        caseOperation),
                BIGINT);
    }

    @Test
    public void testCast()
    {
        io.trino.sql.ir.Cast castExpression = new io.trino.sql.ir.Cast(new io.trino.sql.ir.Constant(SMALLINT, 1L), BIGINT);

        Constant constantOperation = new Constant("%0", SMALLINT, 1L);
        Cast castOperation = new Cast(
                "%1",
                constantOperation.result(),
                BIGINT,
                CastKind.CONVERT,
                constantOperation.attributes());

        assertProgram(
                castExpression,
                ImmutableList.of(
                        constantOperation,
                        castOperation),
                BIGINT);
    }

    @Test
    public void testReinterpretCast()
    {
        Type sourceType = createDecimalType(7, 2);
        Type targetType = createDecimalType(12, 2);
        io.trino.sql.ir.Cast castExpression = new io.trino.sql.ir.Cast(
                new io.trino.sql.ir.Constant(sourceType, 123L),
                targetType,
                Kind.REINTERPRET);

        Constant constantOperation = new Constant("%0", sourceType, 123L);
        Cast castOperation = new Cast(
                "%1",
                constantOperation.result(),
                targetType,
                CastKind.REINTERPRET,
                constantOperation.attributes());

        assertProgram(
                castExpression,
                ImmutableList.of(
                        constantOperation,
                        castOperation),
                targetType);
    }

    @Test
    public void testCoalesce()
    {
        io.trino.sql.ir.Coalesce coalesceExpression = new io.trino.sql.ir.Coalesce(
                new io.trino.sql.ir.Constant(BIGINT, null),
                new io.trino.sql.ir.Constant(BIGINT, null),
                new io.trino.sql.ir.Constant(BIGINT, 1L));

        Constant constantOperation1 = new Constant("%0", BIGINT, null);
        Constant constantOperation2 = new Constant("%1", BIGINT, null);
        Constant constantOperation3 = new Constant("%2", BIGINT, 1L);
        Coalesce coalesceOperation = new Coalesce(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                coalesceExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        coalesceOperation),
                BIGINT);
    }

    @Test
    public void testComparison()
    {
        // the canonical form of a comparison is a Call to the operator function (see IrExpressions.comparison).
        // It maps 1-1 to a new IR call operation: there is no dedicated comparison operation.
        Expression comparisonExpression = new io.trino.sql.ir.Call(
                LESS_THAN_BIGINT,
                ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 1L), new io.trino.sql.ir.Constant(BIGINT, 0L)));

        Constant constantOperationLeft = new Constant("%0", BIGINT, 1L);
        Constant constantOperationRight = new Constant("%1", BIGINT, 0L);
        Call comparisonOperation = new Call(
                "%2",
                ImmutableList.of(constantOperationLeft.result(), constantOperationRight.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(constantOperationLeft.attributes(), constantOperationRight.attributes()));

        assertProgram(
                comparisonExpression,
                ImmutableList.of(
                        constantOperationLeft,
                        constantOperationRight,
                        comparisonOperation),
                BOOLEAN);
    }

    @Test
    public void testConstant()
    {
        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, true);

        Constant constantOperation = new Constant("%0", BOOLEAN, true);

        assertProgram(constantExpression, ImmutableList.of(constantOperation), BOOLEAN);
    }

    @Test
    public void testConstantNull()
    {
        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, null);

        Constant constantOperation = new Constant("%0", BOOLEAN, null);

        assertProgram(constantExpression, ImmutableList.of(constantOperation), BOOLEAN);
    }

    @Test
    public void testFieldReference()
    {
        io.trino.sql.ir.FieldReference fieldReferenceExpression = new io.trino.sql.ir.FieldReference(
                new io.trino.sql.ir.Row(
                        ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 0L),
                                new io.trino.sql.ir.Constant(BOOLEAN, true))),
                0);

        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        FieldReference fieldReferenceOperation = new FieldReference(
                "%3",
                rowOperation.result(),
                0,
                rowOperation.attributes());

        assertProgram(
                fieldReferenceExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        rowOperation,
                        fieldReferenceOperation),
                BIGINT);
    }

    @Test
    public void testIn()
    {
        io.trino.sql.ir.In inExpression = new io.trino.sql.ir.In(
                new io.trino.sql.ir.Constant(BIGINT, 1L),
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 0L),
                        new io.trino.sql.ir.Constant(BIGINT, 1L),
                        new io.trino.sql.ir.Constant(BIGINT, 2L)));

        Constant constantOperationValue = new Constant("%0", BIGINT, 1L);
        Constant constantOperation1 = new Constant("%1", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%2", BIGINT, 1L);
        Constant constantOperation3 = new Constant("%3", BIGINT, 2L);
        In inOperation = new In(
                "%4",
                constantOperationValue.result(),
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperationValue.attributes(), constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                inExpression,
                ImmutableList.of(
                        constantOperationValue,
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        inOperation),
                BOOLEAN);
    }

    @Test
    public void testIsNull()
    {
        io.trino.sql.ir.IsNull isNullExpression = new io.trino.sql.ir.IsNull(new io.trino.sql.ir.Constant(BIGINT, null));

        Constant constantOperation = new Constant("%0", BIGINT, null);
        IsNull isNullOperation = new IsNull("%1", constantOperation.result(), constantOperation.attributes());

        assertProgram(
                isNullExpression,
                ImmutableList.of(
                        constantOperation,
                        isNullOperation),
                BOOLEAN);
    }

    @Test
    public void testLambdaWithoutArguments()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(),
                new io.trino.sql.ir.Constant(BIGINT, 5L));

        Constant constantOperation = new Constant("%2", BIGINT, 5L);
        Return returnOperation = new Return("%3", constantOperation.result(), constantOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%1", irType(EMPTY_ROW))),
                        ImmutableList.of(
                                constantOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(), BIGINT));
    }

    @Test
    public void testSimpleLambda()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(new Symbol(BIGINT, "x")),
                new io.trino.sql.ir.Call(
                        LESS_THAN_BIGINT,
                        ImmutableList.of(new Reference(BIGINT, "x"), new io.trino.sql.ir.Constant(BIGINT, 0L))));

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%2", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%3", BIGINT, 0L);
        Call comparisonOperation = new Call(
                "%4",
                ImmutableList.of(fieldReferenceOperationX.result(), constantOperation.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(fieldReferenceOperationX.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%5", comparisonOperation.result(), comparisonOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                constantOperation,
                                comparisonOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BIGINT), BOOLEAN));
    }

    @Test
    public void testCorrelatedLambda()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(
                        new Symbol(BOOLEAN, "x"),
                        new Symbol(BIGINT, "y")),
                new io.trino.sql.ir.Logical(
                        Operator.OR,
                        ImmutableList.of(
                                new Reference(BOOLEAN, "b"), // correlated symbol
                                new Reference(BOOLEAN, "x"), // lambda argument
                                new io.trino.sql.ir.Call(
                                        LESS_THAN_BIGINT,
                                        ImmutableList.of(
                                                new Reference(BIGINT, "a"), // correlated symbol
                                                new Reference(BIGINT, "y")))))); // lambda argument

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BOOLEAN, BIGINT)));
        FieldReference fieldReferenceOperationB = new FieldReference("%2", INPUT_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationX = new FieldReference("%3", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%4", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationY = new FieldReference("%5", lambdaArgument, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Call comparisonOperation = new Call(
                "%6",
                ImmutableList.of(fieldReferenceOperationA.result(), fieldReferenceOperationY.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(fieldReferenceOperationA.attributes(), fieldReferenceOperationY.attributes()));
        Logical logicalOperation = new Logical(
                "%7",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationX.result(), comparisonOperation.result()),
                OR,
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationX.attributes(), comparisonOperation.attributes()));
        Return returnOperation = new Return("%8", logicalOperation.result(), logicalOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationX,
                                fieldReferenceOperationA,
                                fieldReferenceOperationY,
                                comparisonOperation,
                                logicalOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BOOLEAN, BIGINT), BOOLEAN));
    }

    @Test
    public void testLambdaDuplicateArguments()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(
                        new Symbol(BOOLEAN, "x"),
                        new Symbol(BOOLEAN, "x")),
                new Reference(BOOLEAN, "x"));

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BOOLEAN, BOOLEAN)));
        FieldReference fieldReferenceOperation = new FieldReference("%2", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%3", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN));
    }

    @Test
    public void testLet()
    {
        // the bound value is computed in the enclosing block and referenced in the body
        // as the single field of the body block parameter. The body can also reference
        // enclosing symbols through the composed mapping.
        io.trino.sql.ir.Let letExpression = new io.trino.sql.ir.Let(
                new Symbol(BIGINT, "x"),
                new Reference(BIGINT, "a"),
                new io.trino.sql.ir.Call(
                        LESS_THAN_BIGINT,
                        ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "a"))));

        FieldReference fieldReferenceOperationValue = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Block.Parameter letParameter = new Block.Parameter("%1", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%2", letParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%3", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Call callOperation = new Call(
                "%4",
                ImmutableList.of(fieldReferenceOperationX.result(), fieldReferenceOperationA.result()),
                LESS_THAN_BIGINT,
                ImmutableList.of(fieldReferenceOperationX.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%5", callOperation.result(), callOperation.attributes());
        Let letOperation = new Let(
                "%6",
                fieldReferenceOperationValue.result(),
                new Block(
                        Optional.of("^body"),
                        ImmutableList.of(letParameter),
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                fieldReferenceOperationA,
                                callOperation,
                                returnOperation)),
                fieldReferenceOperationValue.attributes());

        assertProgram(
                letExpression,
                ImmutableList.of(
                        fieldReferenceOperationValue,
                        letOperation),
                BOOLEAN);
    }

    @Test
    public void testLogical()
    {
        io.trino.sql.ir.Logical logicalExpression = new io.trino.sql.ir.Logical(
                Operator.AND,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, false)));

        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Logical logicalOperation = new Logical(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                AND,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                logicalExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        logicalOperation),
                BOOLEAN);
    }

    @Test
    public void testNullIf()
    {
        // the desugared form of `NULLIF(0, smallint 1)` with a trivial first operand (see IrExpressions.nullIf):
        // the second operand is cast to the comparison type. It maps 1-1 to new IR operations.
        Expression nullIfExpression = new io.trino.sql.ir.Case(
                ImmutableList.of(new WhenClause(
                        new io.trino.sql.ir.Call(EQUAL_BIGINT, ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 0L),
                                new io.trino.sql.ir.Cast(new io.trino.sql.ir.Constant(SMALLINT, 1L), BIGINT))),
                        new io.trino.sql.ir.Constant(BIGINT, null))),
                new io.trino.sql.ir.Constant(BIGINT, 0L));

        Constant constantOperationFirst = new Constant("%0", BIGINT, 0L);
        Constant constantOperationSecond = new Constant("%1", SMALLINT, 1L);
        Cast castOperation = new Cast("%2", constantOperationSecond.result(), BIGINT, CastKind.CONVERT, constantOperationSecond.attributes());
        Call callOperation = new Call(
                "%3",
                ImmutableList.of(constantOperationFirst.result(), castOperation.result()),
                EQUAL_BIGINT,
                ImmutableList.of(constantOperationFirst.attributes(), castOperation.attributes()));
        Constant constantOperationNull = new Constant("%4", BIGINT, null);
        Constant constantOperationDefault = new Constant("%5", BIGINT, 0L);
        Case caseOperation = new Case(
                "%6",
                ImmutableList.of(callOperation.result()),
                ImmutableList.of(constantOperationNull.result()),
                constantOperationDefault.result(),
                ImmutableList.of(callOperation.attributes(), constantOperationNull.attributes(), constantOperationDefault.attributes()));

        assertProgram(
                nullIfExpression,
                ImmutableList.of(
                        constantOperationFirst,
                        constantOperationSecond,
                        castOperation,
                        callOperation,
                        constantOperationNull,
                        constantOperationDefault,
                        caseOperation),
                BIGINT);
    }

    @Test
    public void testReference()
    {
        Reference referenceExpression = new Reference(BIGINT, "a");

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

        assertProgram(referenceExpression, ImmutableList.of(fieldReferenceOperation), BIGINT);
    }

    @Test
    public void testRow()
    {
        io.trino.sql.ir.Row rowExpression = new io.trino.sql.ir.Row(
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 0L),
                        new io.trino.sql.ir.Constant(BOOLEAN, true)));

        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        assertProgram(
                rowExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        rowOperation),
                anonymousRow(BIGINT, BOOLEAN));
    }

    @Test
    public void testSwitch()
    {
        io.trino.sql.ir.Match switchExpression = new io.trino.sql.ir.Match(
                new io.trino.sql.ir.Constant(BIGINT, 0L),
                ImmutableList.of(
                        equalityClause(new io.trino.sql.ir.Constant(BIGINT, 1L), new io.trino.sql.ir.Constant(BOOLEAN, true)),
                        equalityClause(new io.trino.sql.ir.Constant(BIGINT, 2L), new io.trino.sql.ir.Constant(BOOLEAN, false))),
                new io.trino.sql.ir.Constant(BOOLEAN, null));

        Constant constantOperationOperand = new Constant("%0", BIGINT, 0L);

        Block.Parameter lambdaArgument1 = new Block.Parameter("%2", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperation1 = new FieldReference("%3", lambdaArgument1, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation1 = new Constant("%4", BIGINT, 1L);
        Call comparisonOperation1 = new Call(
                "%5",
                ImmutableList.of(fieldReferenceOperation1.result(), constantOperation1.result()),
                EQUAL_BIGINT,
                ImmutableList.of(fieldReferenceOperation1.attributes(), constantOperation1.attributes()));
        Return returnOperation1 = new Return("%6", comparisonOperation1.result(), comparisonOperation1.attributes());
        Lambda lambdaOperation1 = new Lambda(
                "%1",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument1),
                        ImmutableList.of(
                                fieldReferenceOperation1,
                                constantOperation1,
                                comparisonOperation1,
                                returnOperation1)));

        Block.Parameter lambdaArgument2 = new Block.Parameter("%8", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperation2 = new FieldReference("%9", lambdaArgument2, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation2 = new Constant("%10", BIGINT, 2L);
        Call comparisonOperation2 = new Call(
                "%11",
                ImmutableList.of(fieldReferenceOperation2.result(), constantOperation2.result()),
                EQUAL_BIGINT,
                ImmutableList.of(fieldReferenceOperation2.attributes(), constantOperation2.attributes()));
        Return returnOperation2 = new Return("%12", comparisonOperation2.result(), comparisonOperation2.attributes());
        Lambda lambdaOperation2 = new Lambda(
                "%7",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument2),
                        ImmutableList.of(
                                fieldReferenceOperation2,
                                constantOperation2,
                                comparisonOperation2,
                                returnOperation2)));

        Constant constantOperationThen1 = new Constant("%13", BOOLEAN, true);
        Constant constantOperationThen2 = new Constant("%14", BOOLEAN, false);
        Constant constantOperationDefault = new Constant("%15", BOOLEAN, null);
        Match matchOperation = new Match(
                "%16",
                constantOperationOperand.result(),
                ImmutableList.of(lambdaOperation1.result(), lambdaOperation2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationOperand.attributes(),
                        lambdaOperation1.attributes(),
                        lambdaOperation2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        assertProgram(
                switchExpression,
                ImmutableList.of(
                        constantOperationOperand,
                        lambdaOperation1,
                        lambdaOperation2,
                        constantOperationThen1,
                        constantOperationThen2,
                        constantOperationDefault,
                        matchOperation),
                BOOLEAN);
    }

    @Test
    public void testNoMappingForSymbol()
    {
        Reference referenceExpression = new Reference(BIGINT, "A");
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        // SYMBOL_MAPPING has entries for symbols "a" and "b", but not for "A"
        assertThatThrownBy(() -> referenceExpression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING)))
                .hasMessage("no mapping for symbol A");
    }

    @Test
    public void testAddReturnOperation()
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, true);
        constantExpression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING));
        scalarProgramBuilder.addReturnOperation(blockBuilder);

        Constant constantOperation = new Constant("%0", BOOLEAN, true);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());

        assertThat(blockBuilder.build().operations()).isEqualTo(ImmutableList.of(constantOperation, returnOperation));
    }

    @Test
    public void testAddReturnOperationEmptyBlock()
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        assertThatThrownBy(() -> scalarProgramBuilder.addReturnOperation(blockBuilder))
                .hasMessage("no operations added yet");
    }

    private void assertProgram(Expression expression, List<Operation> expected, Type expectedType)
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));
        expression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING));
        // add a terminal Return operation. It is required to build the Block
        scalarProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();

        // remove the Return operation
        List<Operation> actual = block.operations().subList(0, block.operations().size() - 1);
        assertThat(actual).isEqualTo(expected);

        assertThat(expression.type()).isEqualTo(expectedType);
        assertThat(trinoType(block.getReturnedType())).isEqualTo(expectedType);
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        Symbol operand = new Symbol(value.type(), "operand");
        return new MatchClause(
                new io.trino.sql.ir.Lambda(
                        ImmutableList.of(operand),
                        new io.trino.sql.ir.Call(EQUAL_BIGINT, ImmutableList.of(operand.toSymbolReference(), value))),
                result);
    }
}
