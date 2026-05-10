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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.DType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.operator.gpu.GpuScore;
import io.trino.operator.gpu.regex.GpuRegexTranspiler;
import io.trino.operator.project.InputChannels;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.gpu.GpuTypeConversion.GpuTypeMapping;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
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
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JoniRegexp;
import io.trino.type.LikePattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.operator.gpu.GpuScore.POTENTIAL;
import static io.trino.operator.gpu.GpuScore.PREFERRED;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

/**
 * Compiles Trino Expressions into GPU-executable operations using cuDF.
 */
public class GpuExpressionCompiler
{
    private static final Logger log = Logger.get(GpuExpressionCompiler.class);

    private static final int TINYINT_DECIMAL_DIGITS = 3;
    private static final int SMALLINT_DECIMAL_DIGITS = 5;
    private static final int INTEGER_DECIMAL_DIGITS = 10;
    private static final int BIGINT_DECIMAL_DIGITS = 19;

    public Optional<List<CompiledExpression>> compileExpressions(List<Expression> expressions, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<CompiledExpression> compiledExpressions = ImmutableList.builderWithExpectedSize(expressions.size());
        for (Expression expression : expressions) {
            Optional<CompiledExpression> compiled = compileExpression(expression, layout);
            if (compiled.isEmpty()) {
                return Optional.empty();
            }
            compiledExpressions.add(compiled.get());
        }
        return Optional.of(compiledExpressions.build());
    }

    public Optional<CompiledExpression> compileExpression(Expression expression, Map<Symbol, Integer> layout)
    {
        CompilationVisitor visitor = new CompilationVisitor(layout);
        Optional<CompiledExpression> compiled = expression.accept(visitor, null)
                .map(result -> new CompiledExpression(result.expression(), new InputChannels(ImmutableList.copyOf(visitor.inputChannels)), result.score()));
        if (compiled.isEmpty()) {
            log.debug("Could not compile expression for GPU execution: %s", expression);
        }
        return compiled;
    }

    @VisibleForTesting
    static class CompilationVisitor
            extends IrVisitor<Optional<CompilationResult>, Void>
    {
        private final Map<Symbol, Integer> sourceLayout;
        // Maps each referenced Symbol to a compact, consecutive index (0, 1, 2, ...).
        // GPU operators use this index to look up the corresponding input column in
        // the materialized list of device-resident columns.
        private final Map<Symbol, Integer> compactLayout = new HashMap<>();
        private final List<Integer> inputChannels = new ArrayList<>();

        private CompilationVisitor(Map<Symbol, Integer> sourceLayout)
        {
            this.sourceLayout = requireNonNull(sourceLayout, "sourceLayout is null");
        }

        @Override
        public Optional<CompilationResult> process(Expression node)
        {
            throw new UnsupportedOperationException("Process without context should not be called");
        }

        @Override
        protected Optional<CompilationResult> visitConstant(Constant literal, Void context)
        {
            return toGpuMapping(literal.type())
                    .map(typeMapping -> new CompilationResult(
                            new GpuConstant(typeMapping.toScalar(), Optional.ofNullable(literal.value())),
                            POTENTIAL));
        }

        @Override
        protected Optional<CompilationResult> visitReference(Reference reference, Void context)
        {
            if (!isConvertible(reference.type())) {
                return Optional.empty();
            }
            Symbol symbol = Symbol.from(reference);
            Integer sourceChannel = sourceLayout.get(symbol);
            verify(sourceChannel != null, "Reference %s not present in source layout", symbol);
            int compactField = compactLayout.computeIfAbsent(symbol, _ -> {
                inputChannels.add(sourceChannel);
                return compactLayout.size();
            });
            return Optional.of(new CompilationResult(
                    (_, inputColumns) -> inputColumns.get(compactField).incRefCount(),
                    POTENTIAL));
        }

        @Override
        protected Optional<CompilationResult> visitArray(Array node, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitRow(Row node, Void context)
        {
            // TODO support ROW type
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitFieldReference(FieldReference node, Void context)
        {
            // TODO support ROW type
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitCast(Cast cast, Void context)
        {
            return toDType(cast.expression().type()).flatMap(fromDType ->
                    toDType(cast.type()).flatMap(toDType ->
                            cast.expression().accept(this, context).flatMap(compiledArgument ->
                                    compileCast(compiledArgument.expression(), cast.expression().type(), fromDType, cast.type(), toDType)
                                            .map(gpuCast -> new CompilationResult(gpuCast, compiledArgument.score())))));
        }

        private static Optional<GpuExpression> compileCast(GpuExpression input, Type fromType, DType fromDType, Type toType, DType toDType)
        {
            if (fromType.equals(toType)) {
                return Optional.of(input);
            }
            return switch (fromType) {
                case TinyintType _ -> switch (toType) {
                    case SmallintType _, IntegerType _, BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when TINYINT_DECIMAL_DIGITS <= d.getPrecision() - d.getScale() -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when TINYINT_DECIMAL_DIGITS + 1 /*sign*/ <= v.getLength().orElse(Integer.MAX_VALUE) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case SmallintType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT16, DType.INT8, "tinyint"));
                    case IntegerType _, BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when SMALLINT_DECIMAL_DIGITS <= d.getPrecision() - d.getScale() -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when SMALLINT_DECIMAL_DIGITS + 1 /*sign*/ <= v.getLength().orElse(Integer.MAX_VALUE) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case IntegerType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT32, DType.INT8, "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT32, DType.INT16, "smallint"));
                    case BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when INTEGER_DECIMAL_DIGITS <= d.getPrecision() - d.getScale() -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when INTEGER_DECIMAL_DIGITS + 1 /*sign*/ <= v.getLength().orElse(Integer.MAX_VALUE) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case BigintType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT8, "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT16, "smallint"));
                    case IntegerType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT32, "integer"));
                    case RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when BIGINT_DECIMAL_DIGITS <= d.getPrecision() - d.getScale() -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when BIGINT_DECIMAL_DIGITS + 1 /*sign*/ <= v.getLength().orElse(Integer.MAX_VALUE) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case RealType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT32, DType.INT8, "real", "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT32, DType.INT16, "real", "smallint"));
                    case IntegerType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT32, DType.INT32, "real", "integer"));
                    case BigintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT32, DType.INT64, "real", "bigint"));
                    case DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case DoubleType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT64, DType.INT8, "double", "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT64, DType.INT16, "double", "smallint"));
                    case IntegerType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT64, DType.INT32, "double", "integer"));
                    case BigintType _ -> Optional.of(new GpuFloatingToIntegerCast(input, DType.FLOAT64, DType.INT64, "double", "bigint"));
                    case RealType _ -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case DecimalType from -> switch (toType) {
                    case TinyintType _ when from.getScale() == 0 && from.getPrecision() < TINYINT_DECIMAL_DIGITS -> Optional.of(new GpuCast(input, toDType));
                    case SmallintType _ when from.getScale() == 0 && from.getPrecision() < SMALLINT_DECIMAL_DIGITS -> Optional.of(new GpuCast(input, toDType));
                    case IntegerType _ when from.getScale() == 0 && from.getPrecision() < INTEGER_DECIMAL_DIGITS -> Optional.of(new GpuCast(input, toDType));
                    case BigintType _ when from.getScale() == 0 && from.getPrecision() < BIGINT_DECIMAL_DIGITS -> Optional.of(new GpuCast(input, toDType));
                    case RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    // target has at least as many fractional digits (no rounding) and integer digits (no overflow)
                    case DecimalType to when from.getScale() <= to.getScale() &&
                            from.getPrecision() - from.getScale() <= to.getPrecision() - to.getScale() -> {
                        if (fromDType.equals(toDType)) {
                            yield Optional.of(input);
                        }
                        yield Optional.of(new GpuCast(input, toDType));
                    }
                    default -> Optional.empty();
                };
                case VarcharType from -> switch (toType) {
                    // No truncation
                    case VarcharType to when to.isUnbounded() ||
                            (!from.isUnbounded() && from.getBoundedLength() <= to.getBoundedLength()) -> {
                        verify(DType.STRING.equals(fromDType) && DType.STRING.equals(toDType), "Unexpected from/to DTypes: %s, %s", fromDType, toDType);
                        yield Optional.of(input);
                    }
                    default -> Optional.empty();
                };
                default -> Optional.empty();
            };
        }

        @Override
        protected Optional<CompilationResult> visitCall(Call call, Void context)
        {
            CatalogSchemaFunctionName functionName = call.function().signature().getName();
            if (!isBuiltinFunctionName(functionName)) {
                return Optional.empty();
            }
            String name = functionName.functionName();

            if (name.equals(LIKE_FUNCTION_NAME) &&
                    call.arguments().size() == 2 &&
                    call.arguments().get(1) instanceof Constant(Type patternType, Object likePattern) &&
                    patternType == LIKE_PATTERN) {
                return call.arguments().get(0).accept(this, context)
                        .map(searched -> new CompilationResult(
                                new GpuLike(searched.expression(), ((LikePattern) likePattern).getPattern(), ((LikePattern) likePattern).getEscape()),
                                Ordering.natural().max(searched.score(), PREFERRED)));
            }

            if (name.equals("$not") && call.arguments().size() == 1) {
                return call.arguments().getFirst().accept(this, context)
                        .map(operand -> new CompilationResult(
                                new GpuNot(operand.expression()),
                                operand.score()));
            }

            if (isOperatorName(name)) {
                OperatorType operatorType = unmangleOperator(name);
                if (call.arguments().size() == 2) {
                    return compileBinaryArithmetic(call, operatorType, context);
                }
            }

            Optional<GpuDateTimeExtract.Field> dateTimeField = GpuDateTimeExtract.Field.forTrinoFunctionName(name);
            if (dateTimeField.isPresent() && call.arguments().size() == 1) {
                Type argumentType = getOnlyElement(call.arguments()).type();
                if (argumentType == DATE ||
                        argumentType instanceof TimeType ||
                        argumentType instanceof TimeWithTimeZoneType ||
                        argumentType instanceof TimestampType ||
                        argumentType instanceof TimestampWithTimeZoneType ||
                        argumentType instanceof IntervalYearMonthType ||
                        argumentType instanceof IntervalDayTimeType) {
                    return compileDateTimeExtract(call.arguments().getFirst(), dateTimeField.get(), context);
                }
            }

            if (name.equals("length") && call.arguments().size() == 1 && getOnlyElement(call.arguments()).type() instanceof VarcharType) {
                return getOnlyElement(call.arguments()).accept(this, context)
                        .map(compiled -> new CompilationResult(
                                new GpuStringLength(compiled.expression()),
                                compiled.score()));
            }

            // TODO: add substring support for char(x)
            if (name.equals("substring") && call.arguments().getFirst().type() instanceof VarcharType) {
                return compileSubstring(call, context);
            }

            if (name.equals("regexp_replace")) {
                return compileRegexpReplace(call, context);
            }

            return Optional.empty();
        }

        private Optional<CompilationResult> compileBinaryArithmetic(Call call, OperatorType operatorType, Void context)
        {
            Optional<DType> outputTypeOpt = toDType(call.type());
            if (outputTypeOpt.isEmpty()) {
                return Optional.empty();
            }
            DType outputType = outputTypeOpt.get();

            Type leftType = call.arguments().get(0).type();
            Type rightType = call.arguments().get(1).type();

            Optional<List<CompilationResult>> argsOpt = compileAll(call.arguments(), context);
            if (argsOpt.isEmpty()) {
                return Optional.empty();
            }
            List<CompilationResult> args = argsOpt.get();
            GpuExpression left = args.get(0).expression();
            GpuExpression right = args.get(1).expression();

            Optional<GpuExpression> gpuExpression = switch (operatorType) {
                case ADD -> {
                    if (leftType == TINYINT && rightType == TINYINT) {
                        yield Optional.of(new GpuIntegerAdd(left, right, outputType, "tinyint"));
                    }
                    if (leftType == SMALLINT && rightType == SMALLINT) {
                        yield Optional.of(new GpuIntegerAdd(left, right, outputType, "smallint"));
                    }
                    if (leftType == INTEGER && rightType == INTEGER) {
                        yield Optional.of(new GpuIntegerAdd(left, right, outputType, "integer"));
                    }
                    if (leftType == BIGINT && rightType == BIGINT) {
                        yield Optional.of(new GpuIntegerAdd(left, right, outputType, "bigint"));
                    }
                    if (leftType == REAL && rightType == REAL) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.ADD, outputType));
                    }
                    if (leftType == DOUBLE && rightType == DOUBLE) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.ADD, outputType));
                    }
                    yield Optional.empty();
                }
                case SUBTRACT -> {
                    if (leftType == TINYINT && rightType == TINYINT) {
                        yield Optional.of(new GpuIntegerSubtract(left, right, outputType, "tinyint"));
                    }
                    if (leftType == SMALLINT && rightType == SMALLINT) {
                        yield Optional.of(new GpuIntegerSubtract(left, right, outputType, "smallint"));
                    }
                    if (leftType == INTEGER && rightType == INTEGER) {
                        yield Optional.of(new GpuIntegerSubtract(left, right, outputType, "integer"));
                    }
                    if (leftType == BIGINT && rightType == BIGINT) {
                        yield Optional.of(new GpuIntegerSubtract(left, right, outputType, "bigint"));
                    }
                    if (leftType == REAL && rightType == REAL) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.SUB, outputType));
                    }
                    if (leftType == DOUBLE && rightType == DOUBLE) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.SUB, outputType));
                    }
                    yield Optional.empty();
                }
                case MULTIPLY -> {
                    if (leftType == TINYINT && rightType == TINYINT) {
                        yield Optional.of(new GpuIntegerMultiply(left, right, outputType, DType.INT16, "tinyint"));
                    }
                    if (leftType == SMALLINT && rightType == SMALLINT) {
                        yield Optional.of(new GpuIntegerMultiply(left, right, outputType, DType.INT32, "smallint"));
                    }
                    if (leftType == INTEGER && rightType == INTEGER) {
                        yield Optional.of(new GpuIntegerMultiply(left, right, outputType, DType.INT64, "integer"));
                    }
                    if (leftType == BIGINT && rightType == BIGINT) {
                        // TODO (https://starburstdata.atlassian.net/browse/ENG-12005) Optimize GPU BIGINT multiply overflow detection
                        yield Optional.of(new GpuIntegerMultiply(left, right, outputType, DType.create(DType.DTypeEnum.DECIMAL128, 0), "bigint"));
                    }
                    if (leftType == REAL && rightType == REAL) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MUL, outputType));
                    }
                    if (leftType == DOUBLE && rightType == DOUBLE) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MUL, outputType));
                    }
                    if (leftType instanceof DecimalType leftDecimal && rightType instanceof DecimalType rightDecimal
                            && leftDecimal.isShort() && rightDecimal.isShort()) {
                        if (call.type() instanceof DecimalType resultDecimal && resultDecimal.isShort()) {
                            // Infallible: the planner derives r_precision >= a_precision + b_precision,
                            // so the result type is always wide enough for any product. The CPU's
                            // multiplyShortShortShort confirms this with an unchecked a * b.
                            yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MUL, outputType));
                        }
                        // Result overflows DECIMAL64, widen inputs to DECIMAL128 before multiplying
                        yield Optional.of(new GpuWideningShortDecimalMultiply(left, right, outputType));
                    }

                    yield Optional.empty();
                }
                case DIVIDE -> {
                    if (leftType == TINYINT && rightType == TINYINT) {
                        yield Optional.of(new GpuIntegerDivide(left, right, outputType, "tinyint"));
                    }
                    if (leftType == SMALLINT && rightType == SMALLINT) {
                        yield Optional.of(new GpuIntegerDivide(left, right, outputType, "smallint"));
                    }
                    if (leftType == INTEGER && rightType == INTEGER) {
                        yield Optional.of(new GpuIntegerDivide(left, right, outputType, "integer"));
                    }
                    if (leftType == BIGINT && rightType == BIGINT) {
                        yield Optional.of(new GpuIntegerDivide(left, right, outputType, "bigint"));
                    }
                    if (leftType == REAL && rightType == REAL) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.DIV, outputType));
                    }
                    if (leftType == DOUBLE && rightType == DOUBLE) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.DIV, outputType));
                    }
                    yield Optional.empty();
                }
                case MODULUS -> {
                    if (leftType == TINYINT && rightType == TINYINT) {
                        yield Optional.of(new GpuIntegerModulo(left, right, outputType));
                    }
                    if (leftType == SMALLINT && rightType == SMALLINT) {
                        yield Optional.of(new GpuIntegerModulo(left, right, outputType));
                    }
                    if (leftType == INTEGER && rightType == INTEGER) {
                        yield Optional.of(new GpuIntegerModulo(left, right, outputType));
                    }
                    if (leftType == BIGINT && rightType == BIGINT) {
                        yield Optional.of(new GpuIntegerModulo(left, right, outputType));
                    }
                    if (leftType == REAL && rightType == REAL) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MOD, outputType));
                    }
                    if (leftType == DOUBLE && rightType == DOUBLE) {
                        yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MOD, outputType));
                    }
                    yield Optional.empty();
                }
                default -> Optional.empty();
            };
            return gpuExpression.map(expression -> new CompilationResult(expression, maxScore(args, POTENTIAL)));
        }

        private Optional<CompilationResult> compileDateTimeExtract(Expression argument, GpuDateTimeExtract.Field field, Void context)
        {
            Type argumentType = argument.type();
            if (argumentType == DATE || argumentType instanceof TimestampType) {
                // For DATE and TIMESTAMP, cudf semantics match Trino's
                return argument.accept(this, context)
                        .map(compiled -> new CompilationResult(
                                new GpuDateTimeExtract(compiled.expression(), field),
                                compiled.score()));
            }
            return Optional.empty();
        }

        private Optional<CompilationResult> compileSubstring(Call call, Void context)
        {
            int argCount = call.arguments().size();
            // substring has only 2-arg (source, start) and 3-arg (source, start, length) overloads
            if (argCount < 2 || argCount > 3) {
                return Optional.empty();
            }

            Optional<CompilationResult> sourceCompiled = call.arguments().get(0).accept(this, context);
            if (sourceCompiled.isEmpty()) {
                return Optional.empty();
            }
            Optional<CompilationResult> startCompiled = call.arguments().get(1).accept(this, context);
            if (startCompiled.isEmpty()) {
                return Optional.empty();
            }

            Optional<GpuExpression> lengthExpression = Optional.empty();
            List<CompilationResult> results = ImmutableList.of(sourceCompiled.get(), startCompiled.get());
            if (argCount == 3) {
                Optional<CompilationResult> lengthCompiled = call.arguments().get(2).accept(this, context);
                if (lengthCompiled.isEmpty()) {
                    return Optional.empty();
                }
                lengthExpression = Optional.of(lengthCompiled.get().expression());
                results = ImmutableList.of(sourceCompiled.get(), startCompiled.get(), lengthCompiled.get());
            }

            return Optional.of(new CompilationResult(
                    new GpuSubstring(sourceCompiled.get().expression(), startCompiled.get().expression(), lengthExpression),
                    maxScore(results, POTENTIAL)));
        }

        private Optional<CompilationResult> compileRegexpReplace(Call call, Void context)
        {
            int argCount = call.arguments().size();
            // defensive check: regexp_replace has only 2- and 3-arg overloads
            if (argCount < 2 || argCount > 3) {
                return Optional.empty();
            }

            if (!(call.arguments().get(1) instanceof Constant(Type patternType, Object patternValue))) {
                return Optional.empty();
            }

            Optional<String> patternString = extractPatternString(patternType, patternValue);
            if (patternString.isEmpty()) {
                return Optional.empty();
            }

            // 2-arg overload removes all matches, equivalent to replacing with empty string
            String replacementString = "";
            if (argCount == 3) {
                if (!(call.arguments().get(2) instanceof Constant(VarcharType _, Object replacementValue)) || replacementValue == null) {
                    return Optional.empty();
                }
                replacementString = ((Slice) replacementValue).toStringUtf8();
            }

            Optional<GpuRegexTranspiler.TranspileResult> transpiled = GpuRegexTranspiler.transpile(patternString.get(), replacementString);
            if (transpiled.isEmpty()) {
                return Optional.empty();
            }

            GpuRegexTranspiler.TranspileResult result = transpiled.get();
            return call.arguments().getFirst().accept(this, context)
                    .map(source -> new CompilationResult(
                            new GpuRegexpReplace(source.expression(), result.pattern(), result.replacement(), result.hasBackreferences()),
                            Ordering.natural().max(source.score(), PREFERRED)));
        }

        private static Optional<String> extractPatternString(Type patternType, Object patternValue)
        {
            if (patternType == JONI_REGEXP && patternValue instanceof JoniRegexp joniRegexp) {
                return Optional.of(joniRegexp.pattern().toStringUtf8());
            }
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitLambda(Lambda lambda, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitBind(Bind node, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitComparison(Comparison comparison, Void context)
        {
            verify(comparison.type() == BOOLEAN, "Unexpected comparison type: %s", comparison.type());
            if (toDType(comparison.left().type()).filter(DType::isNestedType).isPresent()) {
                return Optional.empty();
            }
            Optional<CompilationResult> leftCompiled = comparison.left().accept(this, context);
            if (leftCompiled.isEmpty()) {
                return Optional.empty();
            }
            Optional<CompilationResult> rightCompiled = comparison.right().accept(this, context);
            if (rightCompiled.isEmpty()) {
                return Optional.empty();
            }

            GpuExpression left = leftCompiled.get().expression();
            GpuExpression right = rightCompiled.get().expression();
            Optional<GpuExpression> compiledComparison = switch (comparison.operator()) {
                case EQUAL -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.EQUAL, DType.BOOL8));
                case NOT_EQUAL -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.NOT_EQUAL, DType.BOOL8));
                case LESS_THAN -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.LESS, DType.BOOL8));
                case LESS_THAN_OR_EQUAL -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.LESS_EQUAL, DType.BOOL8));
                case GREATER_THAN -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.GREATER, DType.BOOL8));
                case GREATER_THAN_OR_EQUAL -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.GREATER_EQUAL, DType.BOOL8));
                case IDENTICAL -> Optional.empty();
            };

            return compiledComparison.map(expression -> new CompilationResult(expression, maxScore(List.of(leftCompiled.get(), rightCompiled.get()), POTENTIAL)));
        }

        @Override
        protected Optional<CompilationResult> visitBetween(Between between, Void context)
        {
            if (toDType(between.value().type()).filter(DType::isNestedType).isPresent()) {
                return Optional.empty();
            }
            return compileNary(
                    ImmutableList.of(between.value(), between.min(), between.max()),
                    args -> new GpuBetween(args.get(0), args.get(1), args.get(2)),
                    context);
        }

        @Override
        protected Optional<CompilationResult> visitIn(In in, Void context)
        {
            Optional<GpuTypeMapping> typeMapping = toGpuMapping(in.value().type());
            if (typeMapping.isEmpty()) {
                return Optional.empty();
            }
            if (typeMapping.get().dType().isNestedType()) {
                return Optional.empty();
            }

            Optional<CompilationResult> valueCompiled = in.value().accept(this, context);
            if (valueCompiled.isEmpty()) {
                return Optional.empty();
            }

            // Currently, we support only constants in the value list
            boolean hasNull = false;
            ImmutableList.Builder<Object> nonNullConstants = ImmutableList.builder();
            for (Expression item : in.valueList()) {
                if (!(item instanceof Constant constant)) {
                    return Optional.empty();
                }
                Object value = constant.value();
                if (value == null) {
                    hasNull = true;
                }
                else {
                    nonNullConstants.add(value);
                }
            }

            return Optional.of(new CompilationResult(
                    new GpuIn(valueCompiled.get().expression(), nonNullConstants.build(), hasNull, in.value().type(), typeMapping.get().toColumn()),
                    Ordering.natural().max(valueCompiled.get().score(), POTENTIAL)));
        }

        @Override
        protected Optional<CompilationResult> visitIsNull(IsNull isNull, Void context)
        {
            return isNull.value().accept(this, context)
                    .map(operand -> new CompilationResult(
                            new GpuIsNull(operand.expression()),
                            Ordering.natural().max(operand.score(), POTENTIAL)));
        }

        @Override
        protected Optional<CompilationResult> visitLogical(Logical logical, Void context)
        {
            Function<List<GpuExpression>, GpuExpression> constructor = switch (logical.operator()) {
                case AND -> GpuLogicalExpression::and;
                case OR -> GpuLogicalExpression::or;
            };
            return compileNary(logical.terms(), constructor, context);
        }

        @Override
        protected Optional<CompilationResult> visitCase(Case caseExpression, Void context)
        {
            // Only the IF-equivalent shape (single WhenClause + default) is supported. Multi-branch CASE
            // needs first-class GPU support and is left to a follow-up.
            if (caseExpression.whenClauses().size() != 1) {
                return Optional.empty();
            }
            WhenClause when = caseExpression.whenClauses().getFirst();
            return compileNary(
                    ImmutableList.of(when.getOperand(), when.getResult(), caseExpression.defaultValue()),
                    args -> new GpuIf(args.get(0), args.get(1), args.get(2)),
                    context);
        }

        @Override
        protected Optional<CompilationResult> visitSwitch(Switch node, Void context)
        {
            // TODO support simple CASE on GPU
            return Optional.empty();
        }

        @Override
        protected Optional<CompilationResult> visitCoalesce(Coalesce coalesce, Void context)
        {
            return compileNary(coalesce.operands(), GpuCoalesce::new, context);
        }

        @Override
        protected Optional<CompilationResult> visitNullIf(NullIf node, Void context)
        {
            // TODO support NULLIF on GPU
            return Optional.empty();
        }

        private Optional<CompilationResult> compileNary(
                List<Expression> arguments,
                Function<List<GpuExpression>, GpuExpression> expressionFactory,
                Void context)
        {
            checkArgument(arguments.size() >= 2, "Expression requires at least 2 arguments, got %s", arguments.size());
            return compileAll(arguments, context)
                    .map(results -> new CompilationResult(
                            expressionFactory.apply(results.stream().map(CompilationResult::expression).collect(toImmutableList())),
                            maxScore(results, POTENTIAL)));
        }

        private Optional<List<CompilationResult>> compileAll(List<Expression> expressions, Void context)
        {
            ImmutableList.Builder<CompilationResult> results = ImmutableList.builder();
            for (Expression expression : expressions) {
                Optional<CompilationResult> compiled = expression.accept(this, context);
                if (compiled.isEmpty()) {
                    return Optional.empty();
                }
                results.add(compiled.get());
            }
            return Optional.of(results.build());
        }

        private static GpuScore maxScore(List<CompilationResult> results, GpuScore defaultScore)
        {
            return results.stream()
                    .map(CompilationResult::score)
                    .max(Ordering.natural())
                    .orElse(defaultScore);
        }

        @Override
        protected Optional<CompilationResult> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }
    }

    @VisibleForTesting
    record CompilationResult(GpuExpression expression, GpuScore score)
    {
        public CompilationResult
        {
            requireNonNull(expression, "expression is null");
            requireNonNull(score, "score is null");
        }
    }
}
