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
package io.trino.plugin.warp.expression.rewrite.coordinator.warptonative;

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.type.cast.FromVarchar;
import io.trino.plugin.warp.type.cast.TimestampToVarchar;
import io.trino.plugin.warp.type.cast.ToVarchar;
import io.trino.plugin.warp.type.cast.VarcharToTimestamp;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_NONE;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_VALUES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_VALUES;
import static io.trino.plugin.warp.type.TypeUtils.isArrayType;
import static io.trino.plugin.warp.type.TypeUtils.isBigIntegerType;
import static io.trino.plugin.warp.type.TypeUtils.isBooleanType;
import static io.trino.plugin.warp.type.TypeUtils.isCharType;
import static io.trino.plugin.warp.type.TypeUtils.isDateType;
import static io.trino.plugin.warp.type.TypeUtils.isDoubleType;
import static io.trino.plugin.warp.type.TypeUtils.isIntegerType;
import static io.trino.plugin.warp.type.TypeUtils.isLongDecimalType;
import static io.trino.plugin.warp.type.TypeUtils.isMapType;
import static io.trino.plugin.warp.type.TypeUtils.isRealType;
import static io.trino.plugin.warp.type.TypeUtils.isRowType;
import static io.trino.plugin.warp.type.TypeUtils.isShortDecimalType;
import static io.trino.plugin.warp.type.TypeUtils.isSmallIntType;
import static io.trino.plugin.warp.type.TypeUtils.isStrType;
import static io.trino.plugin.warp.type.TypeUtils.isTimestampType;
import static io.trino.plugin.warp.type.TypeUtils.isTinyIntType;
import static io.trino.plugin.warp.type.TypeUtils.isVarcharType;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class VariableRewriter
        implements ExpressionRewriter<WarpCall>
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(x -> x instanceof WarpVariable));

    private final StorageEngineConstants storageEngineConstants;
    private final PushdownPredicatesStats pushdownPredicatesStats;

    VariableRewriter(StorageEngineConstants storageEngineConstants, PushdownPredicatesStats pushdownPredicatesStats)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.pushdownPredicatesStats = pushdownPredicatesStats;
    }

    static Domain convertSingleValueToDomain(Slice slice, Type columnType, Optional<Integer> castToTypeLengthOptional)
    {
        Object value;
        boolean castIsPossible = true;
        if (isIntegerType(columnType)) {
            value = FromVarchar.toInteger(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromInteger(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isBigIntegerType(columnType)) {
            value = FromVarchar.toBigint(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromBigint(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isRealType(columnType)) {
            value = FromVarchar.toReal(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromReal(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isDoubleType(columnType)) {
            value = FromVarchar.toDouble(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromDouble(slice.length(), (double) value))) {
                castIsPossible = false;
            }
        }
        else if (isBooleanType(columnType)) {
            value = FromVarchar.toBoolean(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromBoolean((boolean) value))) {
                castIsPossible = false;
            }
        }
        else if (isLongDecimalType(columnType)) {
            DecimalType decimalType = (DecimalType) columnType;
            value = FromVarchar.toLongDecimal(slice, decimalType.getPrecision(), decimalType.getScale());
            if (isCastNonInvertible(slice, () -> ToVarchar.fromLongDecimal((Int128) value, decimalType.getScale(), slice.length()))) {
                castIsPossible = false;
            }
        }
        else if (isDateType(columnType)) {
            value = FromVarchar.toDate(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromDate(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isShortDecimalType(columnType)) {
            DecimalType decimalType = (DecimalType) columnType;
            value = FromVarchar.toShortDecimal(slice, decimalType.getPrecision(), decimalType.getScale());
            if (isCastNonInvertible(slice, () -> ToVarchar.fromShortDecimal((long) value, decimalType.getScale(), slice.length()))) {
                castIsPossible = false;
            }
        }
        else if (isTinyIntType(columnType)) {
            value = FromVarchar.toTinyint(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromTinyint(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isSmallIntType(columnType)) {
            value = FromVarchar.toSmallint(slice);
            if (isCastNonInvertible(slice, () -> ToVarchar.fromSmallint(slice.length(), (long) value))) {
                castIsPossible = false;
            }
        }
        else if (isCharType(columnType)) {
            CharType charType = (CharType) columnType;
            if (castToTypeLengthOptional.isPresent() && castToTypeLengthOptional.get() < charType.getLength()) {
                castIsPossible = false;
                value = null;
            }
            else {
                value = slice.length() <= charType.getLength() ? slice : slice.slice(0, charType.getLength());
            }
        }
        else if (isTimestampType(columnType)) {
            TimestampType timestampType = (TimestampType) columnType;
            int precision = timestampType.getPrecision();
            if (precision > 6 && precision <= 12) {
                value = VarcharToTimestamp.castToLongTimestamp(precision, slice.toStringUtf8());
                if (isCastNonInvertible(slice, () -> TimestampToVarchar.cast(precision, (LongTimestamp) value))) {
                    castIsPossible = false;
                }
            }
            else {
                value = VarcharToTimestamp.castToShortTimestamp(precision, slice.toStringUtf8());
                if (isCastNonInvertible(slice, () -> TimestampToVarchar.cast(precision, (long) value))) {
                    castIsPossible = false;
                }
            }
        }
        else {
            String errorMsg = format("cast:: original columnType=%s with cast to castToTypeLengthOptional=%s on valueAsString=%s", columnType, castToTypeLengthOptional, slice.toStringUtf8());
            throw new UnsupportedOperationException(errorMsg);
        }
        return castIsPossible ? Domain.singleValue(columnType, value) : Domain.none(columnType);
    }

    private static <T> boolean isCastNonInvertible(T originalValue, Callable<T> reverseCast)
    {
        try {
            T value = reverseCast.call();
            if (value == null) {
                return originalValue != null;
            }
            return !value.equals(originalValue);
        }
        catch (Exception e) {
            logger.debug(e, "Failed to execute reverse cast - assuming non invertible");
            return true;
        }
    }

    public static PredicateType calcPredicateType(Type constantType, String functionName)
    {
        PredicateType predicateType;
        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME.getName())) {
            if (isStrType(constantType)) {
                predicateType = PREDICATE_TYPE_STRING_VALUES;
            }
            else {
                predicateType = PREDICATE_TYPE_VALUES;
            }
        }
        else {
            if (isStrType(constantType)) {
                predicateType = PREDICATE_TYPE_STRING_RANGES;
            }
            else {
                predicateType = PREDICATE_TYPE_RANGES;
            }
        }
        return predicateType;
    }

    static boolean isCastValidForPushdown(VarcharType castToType, Type columnType)
    {
        int length;
        if (castToType.getLength().isEmpty()) { //unbounded
            return true;
        }
        boolean isValid = true;
        if (columnType instanceof CharType charType) {
            length = charType.getLength();
            if (castToType.getLength().get() < length) {
                isValid = false;
            }
        }
        else if (columnType instanceof VarcharType varcharType) {
            Optional<Integer> optionalLength = varcharType.getLength();
            if (optionalLength.isEmpty()) {
                isValid = false;
            }
            else if (castToType.getLength().get() < optionalLength.get()) {
                isValid = false;
            }
        }
        return isValid;
    }

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    boolean ceil(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_CEIL);
        return true;
    }

    boolean day(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean supported = false;
        if (isValuesPredicateType()) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY);
            supported = true;
        }
        return supported;
    }

    boolean dayOfWeek(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean supported = false;
        if (isValuesPredicateType()) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_WEEK);
            supported = true;
        }
        return supported;
    }

    boolean dayOfYear(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean supported = false;
        if (isValuesPredicateType()) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_DAY_OF_YEAR);
            supported = true;
        }
        return supported;
    }

    boolean week(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean supported = false;
        if (isValuesPredicateType()) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_WEEK);
            supported = true;
        }
        return supported;
    }

    boolean yearOfWeek(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        boolean supported = false;
        if (isValuesPredicateType()) {
            rewriteContext.nativeExpressionBuilder().functionType(FunctionType.FUNCTION_TYPE_YEAR_OF_WEEK);
            supported = true;
        }
        return supported;
    }

    public boolean lower(WarpExpression expression, RewriteContext rewriteContext)
    {
        rewriteContext
                .nativeExpressionBuilder()
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .transformedColumn(TransformFunction.LOWER);
        return true;
    }

    public boolean upper(WarpExpression expression, RewriteContext rewriteContext)
    {
        rewriteContext
                .nativeExpressionBuilder()
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .transformedColumn(TransformFunction.UPPER);
        return true;
    }

    boolean cast(WarpExpression warpExpression, RewriteContext nativeExpressionRewriteContext)
    {
        NativeExpression.Builder nativeExpressionBuilder = nativeExpressionRewriteContext.nativeExpressionBuilder();
        Type columnType = nativeExpressionRewriteContext.columnType();
        if (isArrayType(columnType) ||
                isRowType(columnType) ||
                isMapType(columnType)) {
            pushdownPredicatesStats.incunsupported_functions_native();
            return false;
        }
        Type castToType = warpExpression.getType();
        boolean supported = false;
        if (isVarcharType(castToType)) {
            if (!isCastValidForPushdown((VarcharType) castToType, nativeExpressionRewriteContext.columnType())) {
                return false;
            }
            if (nativeExpressionBuilder.getDomain() == null) {
                nativeExpressionBuilder.functionType(FunctionType.FUNCTION_TYPE_NONE);
                supported = true;
            }
            else if (nativeExpressionBuilder.getDomain().isSingleValue()) {
                //remove the 'cast' operator, and cast the value from Slice to column type
                Slice slice = ((Slice) nativeExpressionBuilder.getDomain().getSingleValue());
                Domain singleValueDomain = convertSingleValueToDomain(slice, columnType, ((VarcharType) castToType).getLength());
                PredicateType predicateType;
                if (singleValueDomain.isNone()) {
                    predicateType = PREDICATE_TYPE_NONE;
                }
                else {
                    predicateType = calcPredicateType(columnType, EQUAL_OPERATOR_FUNCTION_NAME.getName());
                }
                nativeExpressionBuilder.domain(singleValueDomain)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .predicateType(predicateType);
                supported = true;
            }
        }
        else if (isVarcharType(columnType) && isDateType(castToType)) {
            if (nativeExpressionBuilder.getFunctionType() == null) {
                nativeExpressionBuilder.functionType(FunctionType.FUNCTION_TYPE_NONE);
            }
            nativeExpressionBuilder.transformedColumn(TransformFunction.DATE);
            supported = true;
        }
        else {
            int recTypeLength = TypeUtils.getTypeLength(castToType, storageEngineConstants.getVarcharMaxLen());
            RecTypeCode recTypeCode = TypeUtils.convertToRecTypeCode(castToType, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
            nativeExpressionBuilder.functionParams(List.of(recTypeCode.ordinal())).functionType(FunctionType.FUNCTION_TYPE_CAST);
            supported = (recTypeCode != RecTypeCode.REC_TYPE_INVALID);
        }
        if (!supported) {
            pushdownPredicatesStats.incunsupported_functions_native();
        }
        return supported;
    }

    boolean isNan(WarpExpression warpExpression, RewriteContext rewriteContext)
    {
        NativeExpression.Builder nativeExpressionBuilder = rewriteContext.nativeExpressionBuilder();
        nativeExpressionBuilder.functionType(FunctionType.FUNCTION_TYPE_IS_NAN);
        if (rewriteContext.nativeExpressionBuilder().getDomain() == null) { //etc: where is_nan(c1)
            Range range = Range.equal(BooleanType.BOOLEAN, true);
            Domain domain = Domain.create(ValueSet.ofRanges(range), false);
            nativeExpressionBuilder.domain(domain)
                    .collectNulls(false)
                    .predicateType(PREDICATE_TYPE_VALUES);
        }
        return true;
    }

    private boolean isValuesPredicateType()
    {
        return true;
    }
}
