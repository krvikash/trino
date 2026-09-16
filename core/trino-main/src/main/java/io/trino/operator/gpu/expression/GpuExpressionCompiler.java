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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.operator.gpu.regex.GpuRegexTranspiler;
import io.trino.operator.project.InputChannels;
import io.trino.operator.scalar.DivideRoundToScale;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
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
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.CodePointsType;
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
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.spi.gpu.GpuTypeConversion.isConvertible;
import static io.trino.spi.gpu.GpuTypeConversion.toDType;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Compiles Trino Expressions into GPU-executable operations using cuDF.
 */
public final class GpuExpressionCompiler
{
    private GpuExpressionCompiler() {}

    private static final Logger log = Logger.get(GpuExpressionCompiler.class);

    private static final int TINYINT_DECIMAL_DIGITS = 3;
    private static final int SMALLINT_DECIMAL_DIGITS = 5;
    private static final int INTEGER_DECIMAL_DIGITS = 10;
    private static final int BIGINT_DECIMAL_DIGITS = 19;

    public static Optional<CompiledExpression> compileExpression(Expression expression, Map<Symbol, Integer> layout, CharVarcharCoercion charVarcharCoercion)
    {
        CompilationVisitor visitor = new CompilationVisitor(layout, charVarcharCoercion);
        Optional<CompiledExpression> compiled = expression.accept(visitor, null)
                .map(result -> new CompiledExpression(result, new InputChannels(ImmutableList.copyOf(visitor.inputChannels))));
        if (compiled.isEmpty()) {
            log.debug("Could not compile expression for GPU execution: %s", expression);
        }
        return compiled;
    }

    @VisibleForTesting
    static class CompilationVisitor
            extends IrVisitor<Optional<GpuExpression>, Void>
    {
        private final Map<Symbol, Integer> sourceLayout;
        private final CharVarcharCoercion charVarcharCoercion;
        // Maps each referenced Symbol to a compact, consecutive index (0, 1, 2, ...).
        // GPU operators use this index to look up the corresponding input column in
        // the materialized list of device-resident columns.
        private final Map<Symbol, Integer> compactLayout = new HashMap<>();
        private final List<Integer> inputChannels = new ArrayList<>();
        // Symbols bound by enclosing Lets, in binding order (innermost last).
        private final List<Symbol> bindingStack = new ArrayList<>();

        private CompilationVisitor(Map<Symbol, Integer> sourceLayout, CharVarcharCoercion charVarcharCoercion)
        {
            this.sourceLayout = requireNonNull(sourceLayout, "sourceLayout is null");
            this.charVarcharCoercion = requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
        }

        @Override
        public Optional<GpuExpression> process(Expression node)
        {
            throw new UnsupportedOperationException("Process without context should not be called");
        }

        @Override
        protected Optional<GpuExpression> visitConstant(Constant literal, Void context)
        {
            return toGpuMapping(literal.type())
                    .map(_ -> new GpuConstant(literal.type(), Optional.ofNullable(literal.value())));
        }

        @Override
        protected Optional<GpuExpression> visitReference(Reference reference, Void context)
        {
            if (!isConvertible(reference.type())) {
                return Optional.empty();
            }
            Symbol symbol = Symbol.from(reference);
            int binding = bindingStack.lastIndexOf(symbol);
            if (binding >= 0) {
                return Optional.of(new GpuBoundReference(bindingStack.size() - 1 - binding));
            }
            Integer sourceChannel = sourceLayout.get(symbol);
            verify(sourceChannel != null, "Reference %s not present in source layout", symbol);
            int compactField = compactLayout.computeIfAbsent(symbol, _ -> {
                inputChannels.add(sourceChannel);
                return compactLayout.size();
            });
            return Optional.of(new GetColumn(compactField));
        }

        @Override
        protected Optional<GpuExpression> visitArray(Array node, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitRow(Row node, Void context)
        {
            // TODO support ROW type
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitFieldReference(FieldReference node, Void context)
        {
            // TODO support ROW type
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitCast(Cast cast, Void context)
        {
            return toDType(cast.expression().type()).flatMap(fromDType ->
                    toDType(cast.type()).flatMap(toDType ->
                            cast.expression().accept(this, context).flatMap(compiledArgument ->
                                    compileCast(compiledArgument, cast.expression().type(), fromDType, cast.type(), toDType))));
        }

        private Optional<GpuExpression> compileCast(GpuExpression input, Type fromType, DType fromDType, Type toType, DType toDType)
        {
            if (fromType.equals(toType)) {
                return Optional.of(input);
            }
            return switch (fromType) {
                case TinyintType _ -> switch (toType) {
                    case SmallintType _, IntegerType _, BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when canCastIntegerToDecimal(d, TINYINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when canCastIntegerToVarchar(v, TINYINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case SmallintType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT16, DType.INT8, "tinyint"));
                    case IntegerType _, BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when canCastIntegerToDecimal(d, SMALLINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when canCastIntegerToVarchar(v, SMALLINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case IntegerType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT32, DType.INT8, "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT32, DType.INT16, "smallint"));
                    case BigintType _, RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when canCastIntegerToDecimal(d, INTEGER_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when canCastIntegerToVarchar(v, INTEGER_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    default -> Optional.empty();
                };
                case BigintType _ -> switch (toType) {
                    case TinyintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT8, "tinyint"));
                    case SmallintType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT16, "smallint"));
                    case IntegerType _ -> Optional.of(new GpuNarrowingIntegerCast(input, DType.INT64, DType.INT32, "integer"));
                    case RealType _, DoubleType _ -> Optional.of(new GpuCast(input, toDType));
                    case DecimalType d when canCastIntegerToDecimal(d, BIGINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
                    case VarcharType v when canCastIntegerToVarchar(v, BIGINT_DECIMAL_DIGITS) -> Optional.of(new GpuCast(input, toDType));
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
                    case DecimalType to when !from.isShort() && from.getScale() == to.getScale() && to.getPrecision() < from.getPrecision() -> Optional.of(new GpuNarrowingDecimalCast(input, toDType, to.getPrecision()));
                    default -> Optional.empty();
                };
                case CharType from -> switch (toType) {
                    // No truncation
                    case CharType to when from.getLength() <= to.getLength() -> {
                        verify(DType.STRING.equals(fromDType) && DType.STRING.equals(toDType), "Unexpected from/to DTypes: %s, %s", fromDType, toDType);
                        yield Optional.of(input);
                    }
                    case VarcharType to -> {
                        verify(DType.STRING.equals(fromDType) && DType.STRING.equals(toDType), "Unexpected from/to DTypes: %s, %s", fromDType, toDType);
                        yield Optional.of(compileCharToVarcharCast(input, from, to));
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

        /// See [io.trino.operator.scalar.CharToVarcharCast], the CPU counterpart.
        private GpuExpression compileCharToVarcharCast(GpuExpression input, CharType from, VarcharType to)
        {
            int targetLength = to.getLength().orElse(Integer.MAX_VALUE);
            GpuExpression result = input;
            if (targetLength < from.getLength()) {
                result = new GpuTruncateToLength(result, targetLength);
            }
            if (charVarcharCoercion == CharVarcharCoercion.LEGACY) {
                // The GPU carries char(n) without trailing spaces, so the legacy cast has to restore them
                result = new GpuPadSpaces(result, min(from.getLength(), targetLength));
            }
            return result;
        }

        private static boolean canCastIntegerToDecimal(DecimalType type, int decimalDigits)
        {
            return decimalDigits <= type.getPrecision() - type.getScale();
        }

        private static boolean canCastIntegerToVarchar(VarcharType type, int decimalDigits)
        {
            return decimalDigits + 1 /*sign*/ <= type.getLength().orElse(Integer.MAX_VALUE);
        }

        @Override
        protected Optional<GpuExpression> visitCall(Call call, Void context)
        {
            CatalogSchemaFunctionName functionName = call.function().signature().getName();
            if (!isBuiltinFunctionName(functionName)) {
                return Optional.empty();
            }
            String name = functionName.functionName();

            IrExpressions.Comparison comparison = matchComparison(call);
            if (comparison != null) {
                return compileComparison(comparison, context);
            }

            if (name.equals(LIKE_FUNCTION_NAME) &&
                    call.arguments().size() == 2 &&
                    call.arguments().get(1) instanceof Constant(Type patternType, Object likePattern) &&
                    patternType == LIKE_PATTERN) {
                return call.arguments().get(0).accept(this, context)
                        .map(searched -> new GpuLike(searched, ((LikePattern) likePattern).getPattern(), ((LikePattern) likePattern).getEscape()));
            }

            if (name.equals("$not") && call.arguments().size() == 1) {
                return call.arguments().getFirst().accept(this, context)
                        .map(GpuNot::new);
            }

            if (isOperatorName(name)) {
                OperatorType operatorType = unmangleOperator(name);
                if (call.arguments().size() == 2) {
                    return compileBinaryArithmetic(call, operatorType, context);
                }
            }

            if (call.arguments().size() == 1 && isDateTimeType(getOnlyElement(call.arguments()).type())) {
                switch (name) {
                    case "year", "day", "hour", "minute", "second" -> {
                        // This is a date/time extract function
                        return compileDateTimeExtract(name, getOnlyElement(call.arguments()), call.type(), context);
                    }
                    case "last_day_of_month" -> {
                        return compileLastDayOfMonth(getOnlyElement(call.arguments()), context);
                    }
                }
            }

            if (name.equals("length") && call.arguments().size() == 1 && getOnlyElement(call.arguments()).type() instanceof VarcharType) {
                return getOnlyElement(call.arguments()).accept(this, context)
                        .map(GpuStringLength::new);
            }

            if (name.equals("lower") && call.arguments().size() == 1) {
                Expression argument = getOnlyElement(call.arguments());
                if (argument.type() instanceof VarcharType || argument.type() instanceof CharType) {
                    return argument.accept(this, context)
                            .map(GpuLower::new);
                }
            }

            // TODO: add substring support for char(x)
            if (name.equals("substring") && call.arguments().getFirst().type() instanceof VarcharType) {
                return compileSubstring(call, context);
            }

            if (name.equals("concat")) {
                return compileConcat(call, context);
            }

            if (name.equals("regexp_replace")) {
                return compileRegexpReplace(call, context);
            }

            if (name.equals("replace")) {
                return compileReplace(call, context);
            }

            if (name.equals("trim")) {
                return compileTrim(call, GpuTrim.Side.BOTH, context);
            }

            if (name.equals("ltrim")) {
                return compileTrim(call, GpuTrim.Side.LEADING, context);
            }

            if (name.equals("rtrim")) {
                return compileTrim(call, GpuTrim.Side.TRAILING, context);
            }

            if (name.equals("date_trunc") && call.arguments().size() == 2) {
                return compileDateTrunc(call, context);
            }

            if (name.equals("date_add") && call.arguments().size() == 3) {
                return compileDateAdd(call, context);
            }

            if (name.equals(DivideRoundToScale.NAME) && call.arguments().size() == 2) {
                return compileDivideRoundToScale(call, context);
            }

            if (name.equals("least") && call.arguments().size() >= 2) {
                return compileLeast(call, context);
            }

            return Optional.empty();
        }

        private Optional<GpuExpression> compileDivideRoundToScale(Call call, Void context)
        {
            Expression dividend = call.arguments().get(0);
            Expression divisor = call.arguments().get(1);
            // The scalar is currently only called with a decimal(38, s) sum (a DECIMAL128) and a bigint count.
            if (!(dividend.type() instanceof DecimalType dividendType) || dividendType.isShort() || divisor.type() != BIGINT) {
                return Optional.empty();
            }
            Optional<GpuExpression> dividendCompiled = dividend.accept(this, context);
            if (dividendCompiled.isEmpty()) {
                return Optional.empty();
            }
            Optional<GpuExpression> divisorCompiled = divisor.accept(this, context);
            if (divisorCompiled.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new GpuDivideRoundToScale(dividendCompiled.get(), divisorCompiled.get()));
        }

        private Optional<GpuExpression> compileLeast(Call call, Void context)
        {
            // TODO support real and double: Trino orders NaN last, so least(1.0, NaN) is 1.0, while
            //  cudf NULL_MIN yields NaN because NaN compares false against everything.
            switch (call.type()) {
                case BooleanType _,
                     TinyintType _, SmallintType _, IntegerType _, BigintType _,
                     DecimalType _,
                     CharType _, VarcharType _, DateType _ -> {
                    // cudf NULL_MIN semantics for carrier DType match those of Trino Type
                }
                case TimestampType timestampType when timestampType.getPrecision() <= 9 -> {
                    // cudf NULL_MIN semantics for carrier DType match those of Trino Type
                }
                default -> {
                    return Optional.empty();
                }
            }
            Optional<DType> outputType = toDType(call.type());
            if (outputType.isEmpty()) {
                return Optional.empty();
            }
            // cuDF compares raw bytes; Trino CHAR ordering requires virtual right-padding with spaces.
            // Pad operands before comparison, then strip the padding from the result.
            if (call.type() instanceof CharType charType) {
                return compileNary(call.arguments(), operands -> {
                    List<GpuExpression> padded = operands.stream()
                            .map(operand -> (GpuExpression) new GpuPadSpaces(operand, charType.getLength()))
                            .toList();
                    return new GpuTrim(new GpuLeast(padded, outputType.get()), " ", GpuTrim.Side.TRAILING, false);
                }, context);
            }
            return compileNary(call.arguments(), operands -> new GpuLeast(operands, outputType.get()), context);
        }

        private Optional<GpuExpression> compileComparison(IrExpressions.Comparison comparison, Void context)
        {
            switch (comparison.left().type()) {
                case BooleanType _,
                     TinyintType _, SmallintType _, IntegerType _, BigintType _,
                     DecimalType _,
                     CharType _, VarcharType _, DateType _ -> {
                    // cudf comparison semantics for carrier DType match those of Trino Type
                }
                case RealType _, DoubleType _ when !(comparison instanceof IrExpressions.Comparison.Identical) -> {
                    // cudf comparison semantics for carrier DType match those of Trino Type
                }
                case TimestampType timestampType when timestampType.getPrecision() <= 9 -> {
                    // cudf comparison semantics for carrier DType match those of Trino Type
                }
                default -> {
                    return Optional.empty();
                }
            }
            Optional<GpuExpression> leftCompiled = comparison.left().accept(this, context);
            if (leftCompiled.isEmpty()) {
                return Optional.empty();
            }
            Optional<GpuExpression> rightCompiled = comparison.right().accept(this, context);
            if (rightCompiled.isEmpty()) {
                return Optional.empty();
            }

            GpuExpression left = leftCompiled.get();
            GpuExpression right = rightCompiled.get();
            // cuDF compares raw bytes; Trino CHAR ordering requires virtual right-padding with spaces.
            // Equality is unaffected because equal CHAR values have identical raw bytes.
            if (comparison.left().type() instanceof CharType charType
                    && (comparison instanceof IrExpressions.Comparison.LessThan || comparison instanceof IrExpressions.Comparison.LessThanOrEqual)) {
                left = new GpuPadSpaces(left, charType.getLength());
                right = new GpuPadSpaces(right, charType.getLength());
            }
            return switch (comparison) {
                case IrExpressions.Comparison.Equal _ -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.EQUAL, DType.BOOL8));
                case IrExpressions.Comparison.NotEqual _ -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.NOT_EQUAL, DType.BOOL8));
                case IrExpressions.Comparison.LessThan _ -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.LESS, DType.BOOL8));
                case IrExpressions.Comparison.LessThanOrEqual _ -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.LESS_EQUAL, DType.BOOL8));
                case IrExpressions.Comparison.Identical _ -> Optional.of(new GpuBinaryExpression(left, right, BinaryOp.NULL_EQUALS, DType.BOOL8));
            };
        }

        private Optional<GpuExpression> compileBinaryArithmetic(Call call, OperatorType operatorType, Void context)
        {
            Optional<DType> outputTypeOpt = toDType(call.type());
            if (outputTypeOpt.isEmpty()) {
                return Optional.empty();
            }
            DType outputType = outputTypeOpt.get();

            Type leftType = call.arguments().get(0).type();
            Type rightType = call.arguments().get(1).type();

            Optional<List<GpuExpression>> argsOpt = compileAll(call.arguments(), context);
            if (argsOpt.isEmpty()) {
                return Optional.empty();
            }
            List<GpuExpression> args = argsOpt.get();
            GpuExpression left = args.get(0);
            GpuExpression right = args.get(1);

            return switch (operatorType) {
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
                    if (leftType instanceof DecimalType leftDecimal && rightType instanceof DecimalType rightDecimal) {
                        if (leftDecimal.isShort() && rightDecimal.isShort()
                                && call.type() instanceof DecimalType resultDecimal && resultDecimal.isShort()) {
                            // Infallible: the result type is always wide enough for any sum.
                            // The CPU's addShortShortShort confirms this with an unchecked a * aRescale + b * bRescale.
                            yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.ADD, outputType));
                        }
                        if (addSubtractFitsInDecimal128(leftDecimal, rightDecimal)) {
                            yield Optional.of(new GpuWideningShortDecimalArithmetic(left, right, BinaryOp.ADD, outputType));
                        }
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
                    if (leftType instanceof DecimalType leftDecimal && rightType instanceof DecimalType rightDecimal) {
                        if (leftDecimal.isShort() && rightDecimal.isShort()
                                && call.type() instanceof DecimalType resultDecimal && resultDecimal.isShort()) {
                            // Infallible: the result type is always wide enough for any difference.
                            // The CPU's subtractShortShortShort confirms this with an unchecked a * aRescale - b * bRescale.
                            yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.SUB, outputType));
                        }
                        if (addSubtractFitsInDecimal128(leftDecimal, rightDecimal)) {
                            yield Optional.of(new GpuWideningShortDecimalArithmetic(left, right, BinaryOp.SUB, outputType));
                        }
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
                    if (leftType instanceof DecimalType leftDecimal && rightType instanceof DecimalType rightDecimal) {
                        if (leftDecimal.isShort() && rightDecimal.isShort()
                                && call.type() instanceof DecimalType resultDecimal && resultDecimal.isShort()) {
                            // Infallible: the result type is always wide enough for any product.
                            // The CPU's multiplyShortShortShort confirms this with an unchecked a * b.
                            yield Optional.of(new GpuBinaryExpression(left, right, BinaryOp.MUL, outputType));
                        }
                        if (leftDecimal.getPrecision() + rightDecimal.getPrecision() <= 38) {
                            // Product fits in DECIMAL128 without rescaling (resultRescale is always 0
                            // when raw precision <= 38). Widen both inputs to DECIMAL128 before multiplying.
                            yield Optional.of(new GpuWideningShortDecimalArithmetic(left, right, BinaryOp.MUL, outputType));
                        }
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
                    if (leftType instanceof DecimalType dividendType && rightType instanceof DecimalType divisorType
                            && call.type() instanceof DecimalType resultDecimal) {
                        // Same rescaling the CPU applies to the dividend, see DecimalOperators.divideRescaleFactor
                        int rescaleFactor = resultDecimal.getScale() - dividendType.getScale() + divisorType.getScale();
                        if (decimalDivideFitsInDecimal128(dividendType.getPrecision(), rescaleFactor)) {
                            yield Optional.of(new GpuDecimalDivide(left, right, dividendType.getPrecision(), divisorType.getPrecision(), rescaleFactor, outputType));
                        }
                    }
                    yield Optional.empty();
                }
                case MODULO -> {
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
        }

        // Checks whether decimal divide can be carried out in DECIMAL128. GpuDecimalDivide either rescales
        // the dividend up front, which needs dividendPrecision + rescaleFactor digits, or divides first and
        // rescales the remainder, which needs 3 * rescaleFactor digits.
        private static boolean decimalDivideFitsInDecimal128(int dividendPrecision, int rescaleFactor)
        {
            return rescaleFactor >= 0
                    && (dividendPrecision + rescaleFactor <= MAX_PRECISION || 3 * rescaleFactor <= MAX_PRECISION);
        }

        // Checks whether the raw result precision for decimal add/subtract (before capping at 38)
        // fits in DECIMAL128. When it does, cuDF can perform scale alignment and the operation
        // without intermediate overflow, and the output scale matches the result type's scale
        // (no rescaling needed).
        private static boolean addSubtractFitsInDecimal128(DecimalType left, DecimalType right)
        {
            int integral = Math.max(left.getPrecision() - left.getScale(), right.getPrecision() - right.getScale());
            int scale = Math.max(left.getScale(), right.getScale());
            return integral + scale + 1 <= 38;
        }

        private Optional<GpuExpression> compileDateTimeExtract(String trinoFunctionName, Expression argument, Type resultType, Void context)
        {
            Type argumentType = argument.type();
            if (argumentType == DATE || argumentType instanceof TimestampType) {
                // For DATE and TIMESTAMP, cudf semantics match Trino's
                GpuDateTimeExtract.Field dateTimeField;
                switch (trinoFunctionName) {
                    case "year" -> {
                        // YEAR handled separately as it may overflow INT16 result type
                        return toDType(resultType).flatMap(resultDType ->
                                argument.accept(this, context).map(compiled ->
                                        new GpuCast(new GpuYearExtract(compiled), resultDType)));
                    }
                    case "day" -> dateTimeField = GpuDateTimeExtract.Field.DAY;
                    case "hour" -> dateTimeField = GpuDateTimeExtract.Field.HOUR;
                    case "minute" -> dateTimeField = GpuDateTimeExtract.Field.MINUTE;
                    case "second" -> dateTimeField = GpuDateTimeExtract.Field.SECOND;
                    default -> {
                        return Optional.empty();
                    }
                }
                return toDType(resultType).flatMap(resultDType ->
                        argument.accept(this, context).map(compiled ->
                                new GpuCast(new GpuDateTimeExtract(compiled, dateTimeField), resultDType)));
            }
            return Optional.empty();
        }

        private Optional<GpuExpression> compileLastDayOfMonth(Expression argument, Void context)
        {
            Type argumentType = argument.type();
            if (argumentType == DATE || argumentType instanceof TimestampType) {
                return argument.accept(this, context).map(GpuLastDayOfMonth::new);
            }
            return Optional.empty();
        }

        private Optional<GpuExpression> compileDateTrunc(Call call, Void context)
        {
            // date_trunc(unit, date_time)
            if (!(call.arguments().get(0) instanceof Constant(Type unitType, Object unitValue)) ||
                    !(unitType instanceof VarcharType) ||
                    unitValue == null) {
                return Optional.empty();
            }
            String unit = ((Slice) unitValue).toStringUtf8();

            Optional<GpuDateTrunc.Field> field = GpuDateTrunc.Field.forTrinoDateTruncUnit(unit);
            if (field.isEmpty()) {
                return Optional.empty();
            }

            Expression timestampArgument = call.arguments().get(1);
            if (!(timestampArgument.type() instanceof TimestampType)) {
                return Optional.empty();
            }

            return timestampArgument.accept(this, context)
                    .map(compiled -> new GpuDateTrunc(compiled, field.get()));
        }

        private Optional<GpuExpression> compileDateAdd(Call call, Void context)
        {
            // date_add(unit, value, date_time)
            if (!(call.arguments().get(0) instanceof Constant(Type unitType, Object unitValue)) ||
                    !(unitType instanceof VarcharType) ||
                    unitValue == null) {
                return Optional.empty();
            }
            Optional<GpuDateAdd.Unit> unit = GpuDateAdd.Unit.forTrinoDateAddUnit(((Slice) unitValue).toStringUtf8());
            if (unit.isEmpty()) {
                return Optional.empty();
            }

            // Only a constant shift is supported, so that the range of inputs the GPU and the CPU
            // implementations agree on can be determined at compile time.
            if (!(call.arguments().get(1) instanceof Constant(Type valueType, Object valueObject)) ||
                    valueType != BIGINT ||
                    valueObject == null) {
                return Optional.empty();
            }
            long value = (Long) valueObject;

            Expression dateTimeArgument = call.arguments().get(2);
            Type dateTimeType = dateTimeArgument.type();
            if (!dateTimeType.equals(DATE) && !(dateTimeType instanceof TimestampType)) {
                return Optional.empty();
            }
            if (dateTimeType instanceof TimestampType timestampType &&
                    timestampType.getPrecision() < 3 &&
                    unit.get() == GpuDateAdd.Unit.MILLISECOND) {
                // The CPU implementation rounds the result to the type's precision, which a plain shift does not reproduce
                return Optional.empty();
            }

            return toDType(dateTimeType).flatMap(dType ->
                    dateTimeArgument.accept(this, context).flatMap(compiled ->
                            GpuDateAdd.create(compiled, dType, unit.get(), value)));
        }

        private Optional<GpuExpression> compileSubstring(Call call, Void context)
        {
            int argCount = call.arguments().size();
            // substring has only 2-arg (source, start) and 3-arg (source, start, length) overloads
            if (argCount < 2 || argCount > 3) {
                return Optional.empty();
            }

            Optional<GpuExpression> sourceCompiled = call.arguments().get(0).accept(this, context);
            if (sourceCompiled.isEmpty()) {
                return Optional.empty();
            }
            Optional<GpuExpression> startCompiled = call.arguments().get(1).accept(this, context);
            if (startCompiled.isEmpty()) {
                return Optional.empty();
            }

            Optional<GpuExpression> lengthExpression = Optional.empty();
            if (argCount == 3) {
                Optional<GpuExpression> lengthCompiled = call.arguments().get(2).accept(this, context);
                if (lengthCompiled.isEmpty()) {
                    return Optional.empty();
                }
                lengthExpression = Optional.of(lengthCompiled.get());
            }

            return Optional.of(new GpuSubstring(sourceCompiled.get(), startCompiled.get(), lengthExpression));
        }

        private Optional<GpuExpression> compileConcat(Call call, Void context)
        {
            List<Expression> arguments = call.arguments();
            // defensive check: concat is variadic with at least 2 arguments
            if (arguments.size() < 2) {
                return Optional.empty();
            }

            if (arguments.stream().allMatch(argument -> argument.type() instanceof VarcharType)) {
                return compileNary(arguments, GpuVarcharConcat::new, context);
            }

            if (arguments.size() == 2 &&
                    arguments.get(0).type() instanceof CharType leftType &&
                    arguments.get(1).type() instanceof CharType) {
                return compileNary(arguments, compiled -> new GpuCharConcat(compiled.get(0), compiled.get(1), leftType.getLength()), context);
            }

            // The remaining overloads take varbinary (carried as LIST<INT8> on the GPU), array or map arguments
            return Optional.empty();
        }

        private Optional<GpuExpression> compileRegexpReplace(Call call, Void context)
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
                    .map(source -> new GpuRegexpReplace(source, result.pattern(), result.replacement(), result.hasBackreferences()));
        }

        private Optional<GpuExpression> compileReplace(Call call, Void context)
        {
            int argCount = call.arguments().size();
            // defensive check: replace has only 2- and 3-arg overloads
            if (argCount < 2 || argCount > 3) {
                return Optional.empty();
            }

            // defensive check: replace is declared over varchar only
            if (!(call.arguments().getFirst().type() instanceof VarcharType)) {
                return Optional.empty();
            }

            // cuDF only offers a scalar target for literal replacement, so the search string must be known upfront
            Optional<String> searchConstant = extractVarcharConstant(call.arguments().get(1));
            if (searchConstant.isEmpty()) {
                return Optional.empty();
            }
            String search = searchConstant.get();
            // Trino inserts the replacement around every code point when the search string is empty,
            // which is not a substring replacement at all; cuDF rejects an empty target outright
            if (search.isEmpty()) {
                return Optional.empty();
            }

            // 2-arg overload removes all occurrences, equivalent to replacing with an empty string
            String replacement = "";
            if (argCount == 3) {
                Optional<String> replacementConstant = extractVarcharConstant(call.arguments().get(2));
                if (replacementConstant.isEmpty()) {
                    return Optional.empty();
                }
                replacement = replacementConstant.get();
            }

            Optional<GpuExpression> source = call.arguments().getFirst().accept(this, context);
            if (source.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new GpuReplace(source.get(), search, replacement));
        }

        private static Optional<String> extractVarcharConstant(Expression expression)
        {
            if (expression instanceof Constant(VarcharType _, Object value) && value != null) {
                return Optional.of(((Slice) value).toStringUtf8());
            }
            return Optional.empty();
        }

        private Optional<GpuExpression> compileTrim(Call call, GpuTrim.Side side, Void context)
        {
            if (!(call.type() instanceof VarcharType)) {
                // defensive check: trim, ltrim, and rtrim currently return VARCHAR and the code below relies on the return type being VARCHAR
                return Optional.empty();
            }

            List<Expression> arguments = call.arguments();
            // defensive check: trim, ltrim and rtrim have only 1-arg (whitespace) and 2-arg (given code points) overloads
            if (arguments.isEmpty() || arguments.size() > 2) {
                return Optional.empty();
            }

            Type sourceType = arguments.getFirst().type();
            if (!(sourceType instanceof VarcharType) && !(sourceType instanceof CharType)) {
                return Optional.empty();
            }

            Optional<GpuExpression> source = arguments.getFirst().accept(this, context);
            if (source.isEmpty()) {
                return Optional.empty();
            }

            if (arguments.size() == 1) {
                return Optional.of(new GpuTrim(source.get(), GpuTrim.WHITESPACE_CODE_POINTS, side, false));
            }

            // The analyzer wraps the trim characters in a cast to CodePoints, which the optimizer folds
            // when they are constant. Anything else, including a null, has to run on the CPU.
            if (!(arguments.get(1) instanceof Constant(CodePointsType _, Object codePointsValue))
                    || codePointsValue == null) {
                return Optional.empty();
            }

            int[] codePoints = (int[]) codePointsValue;
            for (int codePoint : codePoints) {
                // new String(int[], int, int) throws on a code point above the Unicode range, and silently
                // encodes a surrogate as '?', which would hand cuDF the wrong set. NUL is excluded because
                // cuDF has mishandled it elsewhere and it is not covered by a test here.
                if (codePoint == 0 || !Character.isValidCodePoint(codePoint) || isSurrogate(codePoint)) {
                    return Optional.empty();
                }
            }
            if (codePoints.length == 0) {
                // Removing no code points leaves a varchar unchanged. For char(x) the CPU still applies
                // Chars.trimTrailingSpaces, but a char(x) value is stored without trailing spaces, so that
                // is a no-op too. See the trailing space note below: this rests on behavior upstream
                // considers a bug, so testTrimCodePoints pins it for both types.
                return source;
            }

            // char(x) trim and rtrim additionally strip trailing spaces, because StringFunctions.charTrim
            // and charRightTrim apply Chars.trimTrailingSpaces after removing the code points. That is not
            // a consequence of the result type, which is varchar(x); we do it only to match the CPU, whose
            // own rationale is unclear and which upstream reports as wrong in
            // https://github.com/trinodb/trino/issues/31004. charLeftTrim does not do it, and neither do we.
            boolean removeTrailingSpaces = sourceType instanceof CharType && side != GpuTrim.Side.LEADING;
            return Optional.of(new GpuTrim(source.get(), new String(codePoints, 0, codePoints.length), side, removeTrailingSpaces));
        }

        private static Optional<String> extractPatternString(Type patternType, Object patternValue)
        {
            if (patternType == JONI_REGEXP && patternValue instanceof JoniRegexp joniRegexp) {
                return Optional.of(joniRegexp.pattern().toStringUtf8());
            }
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitLambda(Lambda lambda, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitBind(Bind node, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitLet(Let let, Void context)
        {
            // Compile the value in the enclosing scope; a Let value cannot reference its own binding.
            Optional<GpuExpression> value = let.value().accept(this, context);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            if (bindingStack.contains(let.name())) {
                // This is considered impossible
                log.warn("Duplicate nested Let symbols");
                return Optional.empty();
            }
            bindingStack.addLast(let.name());
            try {
                return let.body().accept(this, context)
                        .map(body -> new GpuLet(value.get(), body));
            }
            finally {
                bindingStack.removeLast();
            }
        }

        @Override
        protected Optional<GpuExpression> visitIn(In in, Void context)
        {
            switch (in.value().type()) {
                case BooleanType _,
                     TinyintType _, SmallintType _, IntegerType _, BigintType _,
                     RealType _, DoubleType _,
                     DecimalType _,
                     CharType _, VarcharType _, DateType _ -> {
                    // cudf comparison semantics for carrier DType match those of Trino Type
                }
                case TimestampType timestampType when timestampType.getPrecision() <= 9 -> {
                    // cudf comparison semantics for carrier DType match those of Trino Type
                }
                default -> {
                    return Optional.empty();
                }
            }

            Optional<GpuExpression> valueCompiled = in.value().accept(this, context);
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

            return Optional.of(new GpuIn(valueCompiled.get(), nonNullConstants.build(), hasNull, in.value().type()));
        }

        @Override
        protected Optional<GpuExpression> visitIsNull(IsNull isNull, Void context)
        {
            return isNull.value().accept(this, context)
                    .map(GpuIsNull::new);
        }

        @Override
        protected Optional<GpuExpression> visitLogical(Logical logical, Void context)
        {
            Function<List<GpuExpression>, GpuExpression> constructor = switch (logical.operator()) {
                case AND -> GpuLogicalExpression::and;
                case OR -> GpuLogicalExpression::or;
            };
            return compileNary(logical.terms(), constructor, context);
        }

        @Override
        protected Optional<GpuExpression> visitCase(Case caseExpression, Void context)
        {
            if (caseExpression.whenClauses().size() == 1) {
                WhenClause when = caseExpression.whenClauses().getFirst();
                return compileNary(
                        ImmutableList.of(when.getOperand(), when.getResult(), caseExpression.defaultValue()),
                        args -> new GpuIf(args.get(0), args.get(1), args.get(2)),
                        context);
            }

            Optional<DType> outputType = toDType(caseExpression.type());
            if (outputType.isEmpty()) {
                return Optional.empty();
            }
            DType outputDType = outputType.get();

            ImmutableList.Builder<Expression> allExpressions = ImmutableList.builder();
            for (WhenClause when : caseExpression.whenClauses()) {
                allExpressions.add(when.getOperand());
                allExpressions.add(when.getResult());
            }
            allExpressions.add(caseExpression.defaultValue());
            int branchCount = caseExpression.whenClauses().size();
            return compileNary(allExpressions.build(), args -> {
                ImmutableList.Builder<GpuExpression> conditions = ImmutableList.builder();
                ImmutableList.Builder<GpuExpression> results = ImmutableList.builder();
                for (int i = 0; i < branchCount; i++) {
                    conditions.add(args.get(2 * i));
                    results.add(args.get(2 * i + 1));
                }

                // default branch
                conditions.add(new GpuConstant(BOOLEAN, Optional.of(true)));
                results.add(args.getLast());

                return new GpuCase(conditions.build(), results.build(), outputDType);
            }, context);
        }

        @Override
        protected Optional<GpuExpression> visitMatch(Match node, Void context)
        {
            // TODO support simple CASE on GPU
            return Optional.empty();
        }

        @Override
        protected Optional<GpuExpression> visitCoalesce(Coalesce coalesce, Void context)
        {
            // All operands share the Coalesce type, so gating on the result type gates every operand.
            // cudf replaceNulls handles every flat carrier DType, but has no specialization for nested
            // ones, such as the LIST<UINT8> carrying varbinary.
            Optional<DType> cudfType = toDType(coalesce.type());
            if (cudfType.isEmpty() || cudfType.get().isNestedType()) {
                return Optional.empty();
            }
            return compileNary(coalesce.operands(), GpuCoalesce::new, context);
        }

        private Optional<GpuExpression> compileNary(
                List<Expression> arguments,
                Function<List<GpuExpression>, GpuExpression> expressionFactory,
                Void context)
        {
            checkArgument(arguments.size() >= 2, "Expression requires at least 2 arguments, got %s", arguments.size());
            return compileAll(arguments, context)
                    .map(expressionFactory);
        }

        private Optional<List<GpuExpression>> compileAll(List<Expression> expressions, Void context)
        {
            ImmutableList.Builder<GpuExpression> results = ImmutableList.builder();
            for (Expression expression : expressions) {
                Optional<GpuExpression> compiled = expression.accept(this, context);
                if (compiled.isEmpty()) {
                    return Optional.empty();
                }
                results.add(compiled.get());
            }
            return Optional.of(results.build());
        }

        @Override
        protected Optional<GpuExpression> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }
    }

    private static boolean isDateTimeType(Type type)
    {
        return type.equals(DATE) ||
                type instanceof TimeType ||
                type instanceof TimeWithTimeZoneType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                type.equals(INTERVAL_DAY_TIME) ||
                type.equals(INTERVAL_YEAR_MONTH);
    }

    private static boolean isSurrogate(int codePoint)
    {
        return MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE;
    }
}
