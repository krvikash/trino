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
package io.trino.plugin.warp.dispatcher.query.classifier;

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.PredicateInfo;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class PredicateUtil
{
    private static final Logger logger = Logger.get(PredicateUtil.class);

    public static final int PREDICATE_HEADER_SIZE = 5; // determined by the storage engine layer and verified to be correct at NativeStorageEngine init

    private PredicateUtil() {}

    static boolean canApplyPredicate(Optional<WarmUpElement> warmUpElement, Type type)
    {
        // No support for: arrays, non-orderable types (since we need the values sorted in the predicate)
        return warmUpElement.isPresent() && !(type instanceof ArrayType) &&
                (type.isOrderable() || warmUpElement.get().getWarpColumn().isTransformedColumn());
    }

    static PredicateData calcPredicateData(NativeExpression nativeExpression, int recTypeLength, boolean transformAllowed, Type columnType, int functionTargetRecTypeLength)
    {
        int numMatchElements;
        int predicateSize = PREDICATE_HEADER_SIZE;
        Domain domain = nativeExpression.domain();
        ValueSet values = domain.getValues();
        checkArgument(values instanceof SortedRangeSet, "unsupported ValueSet %s", values.getClass());
        SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
        numMatchElements = sortedRangeSet.getRangeCount();
        Type type = domain.getType();
        PredicateType predicateType = nativeExpression.predicateType();

        FunctionType functionType = nativeExpression.functionType();
        if (functionType == FunctionType.FUNCTION_TYPE_TRANSFORMED) {
            functionType = FunctionType.FUNCTION_TYPE_NONE;
        }
        // CAST/DAY-WEEK predicates size off the domain's type width, not recTypeLength - see usesDomainWidth().
        int elementRecTypeLength = usesDomainWidth(functionType) ? functionTargetRecTypeLength : recTypeLength;
        if (functionType != FunctionType.FUNCTION_TYPE_NONE) {
            predicateSize += Byte.BYTES;
            // the precision byte is keyed on the source column type, not the CAST target - match it.
            if (columnType instanceof TimestampType) {
                predicateSize += Byte.BYTES;
            }
        }
        if (nativeExpression.functionParams().size() == 1 && functionType == FunctionType.FUNCTION_TYPE_CAST) {
            predicateSize += Integer.BYTES;
        }
        else if (!nativeExpression.functionParams().isEmpty()) {
            throw new UnsupportedOperationException(format("unfamiliar function params nativeExpression=%s", nativeExpression));
        }

        if (numMatchElements > 0) {
            if (TypeUtils.isStrType(type)) {
                if (nativeExpression.allSingleValue()) {
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_STRING_VALUES, "predicateType is not string values as expected");
                    predicateSize += predicateSizeStringValues(numMatchElements);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_STRING;
                    numMatchElements--; // we ignore inifinity
                    predicateSize += predicateSizeStringInverseValues(numMatchElements);
                }
                else {
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_STRING_RANGES, "predicateType is not string ranges as expected");
                    predicateSize += predicateSizeStringRanges(numMatchElements);
                }
            }
            else {
                // @TODO in testMapMultipleMapTypes we have a predicate that is all single but also ranges
                // this is why we check here if type is values and not force it until the issue is fixed
                if (nativeExpression.allSingleValue() && (predicateType == PredicateType.PREDICATE_TYPE_VALUES)) { // values and not ranges
                    predicateSize += predicateSizeValues(numMatchElements, elementRecTypeLength);
                }
                // never promote a CAST predicate to INVERSE_VALUES: native's fill_inverse_values() has
                // no FUNCTION_TYPE_CAST handling and would misread the CAST-target-width payload.
                else if (transformAllowed && functionType != FunctionType.FUNCTION_TYPE_CAST && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES;
                    numMatchElements--; // (-inf, 5),(5, 10),(10, inf) - we ignore infinity and take 5, 10
                    predicateSize += predicateSizeInverseValues(numMatchElements, elementRecTypeLength);
                }
                else {
                    logger.debug("nativeExpression all-single %b predicate-type %s", nativeExpression.allSingleValue(), predicateType);
                    checkArgument(predicateType == PredicateType.PREDICATE_TYPE_RANGES, "predicateType is not ranges as expected");
                    predicateSize += predicateSizeRanges(numMatchElements, elementRecTypeLength);
                }
            }
        }

        PredicateInfo predicateInfo = new PredicateInfo(predicateType, functionType, numMatchElements, nativeExpression.functionParams(), recTypeLength);
        int hashCode = nativeExpression.domain().getValues().isNone() ?
                Objects.hash(domain.getValues().isNone()) :
                Objects.hash(domain.getValues().getRanges().getSpan().isLowUnbounded(), domain.getValues().getRanges().getSpan().isLowUnbounded());
        hashCode = Objects.hash(nativeExpression, columnType, hashCode, predicateType);
        return PredicateData.builder()
                .predicateHashCode(hashCode)
                .isCollectNulls(domain.isNullAllowed())
                .predicateSize(predicateSize)
                .predicateInfo(predicateInfo)
                .columnType(columnType)
                .build();
    }

    static PredicateData calcPredicateData(Domain domain, int recTypeLength, boolean transformAllowed, Type columnType)
    {
        ValueSet values = domain.getValues();
        checkArgument(values instanceof SortedRangeSet, "unsupported ValueSet %s", values.getClass());
        SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
        int numMatchElements = sortedRangeSet.getRangeCount();
        Type type = domain.getType();
        int predicateSize = PREDICATE_HEADER_SIZE;
        PredicateType predicateType = PredicateType.PREDICATE_TYPE_NONE;

        if (numMatchElements > 0) {
            if (TypeUtils.isStrType(type)) {
                if (isAllSingleValue(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_STRING_VALUES;
                    predicateSize += predicateSizeStringValues(numMatchElements);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_STRING;
                    numMatchElements--; // we ignore inifinity
                    predicateSize += predicateSizeStringInverseValues(numMatchElements);
                }
                else {
                    predicateType = PredicateType.PREDICATE_TYPE_STRING_RANGES;
                    predicateSize += predicateSizeStringRanges(numMatchElements);
                }
            }
            else {
                if (isAllSingleValue(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_VALUES;
                    predicateSize += predicateSizeValues(numMatchElements, recTypeLength);
                }
                else if (transformAllowed && isInversePredicate(sortedRangeSet, type)) {
                    predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES;
                    numMatchElements--; // (-inf, 5),(5, 10),(10, inf) - we ignore infinity and take 5, 10
                    predicateSize += predicateSizeInverseValues(numMatchElements, recTypeLength);
                }
                else {
                    predicateType = PredicateType.PREDICATE_TYPE_RANGES;
                    predicateSize += predicateSizeRanges(numMatchElements, recTypeLength);
                }
            }
        }

        PredicateInfo predicateInfo = new PredicateInfo(predicateType, FunctionType.FUNCTION_TYPE_NONE, numMatchElements, Collections.emptyList(), recTypeLength);
        int hashCode = domain.getValues().isNone() ?
                Objects.hash(domain.getValues().isNone()) :
                Objects.hash(domain.getValues().getRanges().getSpan().isLowUnbounded(), domain.getValues().getRanges().getSpan().isLowUnbounded());
        hashCode = Objects.hash(domain, columnType, hashCode, predicateType);
        return PredicateData
                .builder()
                .predicateHashCode(hashCode)
                .isCollectNulls(domain.isNullAllowed())
                .predicateSize(predicateSize)
                .predicateInfo(predicateInfo)
                .columnType(columnType)
                .build();
    }

    static PredicateType calcPredicateType(Domain domain, Type columnType)
    {
        return calcPredicateData(domain, 0, false, columnType).getPredicateInfo().predicateType();
    }

    private static int predicateSizeStringValues(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements + 2, Long.BYTES); // +2 for the min/max
    }

    private static int predicateSizeStringInverseValues(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements * 2, Long.BYTES); // *2 since for inverse string we put crc and str2int
    }

    private static int predicateSizeStringRanges(int numMatchElements)
    {
        return Math.multiplyExact(numMatchElements * 2, Long.BYTES + 1); // *2 for range, +1 for inclusive/exclusive
    }

    private static int predicateSizeValues(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements, recTypeLength);
    }

    private static int predicateSizeInverseValues(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements, recTypeLength);
    }

    private static int predicateSizeRanges(int numMatchElements, int recTypeLength)
    {
        return Math.multiplyExact(numMatchElements * 2, recTypeLength + 1); // *2 for range, +1 for inclusive/exclusive
    }

    // check if we can inverse the predicate since its more efficient in native.
    // for example: x != 7 (PredicateType.PREDICATE_TYPE_RANGES) to col1 = 7 (predicateType = PredicateType.PREDICATE_TYPE_INVERSE_VALUES)
    // This can be done only if match collect isn't enabled - this condition is checked by the caller
    public static boolean isInversePredicate(SortedRangeSet sortedRangeSet, Type type)
    {
        int rangeCount = sortedRangeSet.getRangeCount();
        if (TypeUtils.isRealType(type) || TypeUtils.isBooleanType(type) || TypeUtils.isDoubleType(type)) {
            return false;
        }

        if (rangeCount <= 1) {
            return false;
        }
        /* this supports the formats:
         * col != 'value'
         * col NOT IN ('value1', 'value2')
         *
         * the case of col <= 3 AND col >= 5 which implies col != 4 is not supported
         */
        for (boolean isInclusive : sortedRangeSet.getInclusive()) {
            if (isInclusive) {
                return false;
            }
        }
        // the set is a complement of single values iff its complement is a discrete set
        return sortedRangeSet.complement().isDiscreteSet();
    }

    // delegates the single-value scan to SortedRangeSet, which memoizes it per instance
    public static boolean isAllSingleValue(SortedRangeSet sortedRangeSet, Type type)
    {
        if (!isValuesEncodingSupported(type)) {
            return false;
        }
        // a null-only domain holds no range that could disqualify it, while isDiscreteSet() reports false on an empty set
        if (sortedRangeSet.getRangeCount() == 0) {
            return true;
        }
        return sortedRangeSet.isDiscreteSet();
    }

    // types without a native values-encoding fill path stay as ranges
    private static boolean isValuesEncodingSupported(Type type)
    {
        return TypeUtils.isStrType(type) ||
                TypeUtils.isLongDecimalType(type) ||
                TypeUtils.isLongType(type) ||
                TypeUtils.isShortDecimalType(type) ||
                TypeUtils.isIntegerType(type) ||
                TypeUtils.isDateType(type) ||
                TypeUtils.isBooleanType(type) ||
                TypeUtils.isDoubleType(type) ||
                TypeUtils.isRealType(type) ||
                TypeUtils.isSmallIntType(type) ||
                TypeUtils.isTinyIntType(type);
    }

    // A predicate's Domain (io.trino.spi.predicate.Domain) pairs the matched ValueSet with the Type
    // those values are expressed in. Usually that Type matches the source column, so recTypeLength
    // (the column's on-disk width) is also the payload width the fillers write at. CAST and the
    // DAY/WEEK family (day(), day_of_week(), day_of_year(), week(), year_of_week()) are exceptions:
    // Trino gives the domain the function's result type - e.g. bigint for day_of_year(date_col),
    // not the DATE column's 4-byte width - so the buffer must be sized off domain.getType() instead.
    public static boolean usesDomainWidth(FunctionType functionType)
    {
        return switch (functionType) {
            case FUNCTION_TYPE_CAST, FUNCTION_TYPE_DAY, FUNCTION_TYPE_DAY_OF_WEEK, FUNCTION_TYPE_DAY_OF_YEAR, FUNCTION_TYPE_WEEK, FUNCTION_TYPE_YEAR_OF_WEEK -> true;
            default -> false;
        };
    }

    public static boolean canMapMatchCollect(Type type, PredicateType predicateType, FunctionType functionType, int numValues)
    {
        return TypeUtils.isMappedMatchCollectSupportedTypes(type) &&
                ((predicateType == PredicateType.PREDICATE_TYPE_VALUES) || (predicateType == PredicateType.PREDICATE_TYPE_STRING_VALUES)) &&
                (functionType == FunctionType.FUNCTION_TYPE_NONE) &&
                (numValues <= GlobalConfig.MAX_NUMBER_OF_MAPPED_MATCH_COLLECT_ELEMENTS);
    }
}
