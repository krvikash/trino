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

import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.read.predicates.RangesPredicateFiller;
import io.trino.plugin.warp.storage.read.predicates.ValuesPredicateFiller;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Allocates exactly the size {@link PredicateUtil#calcPredicateData} computes and runs the real
 * {@link RangesPredicateFiller} or {@link ValuesPredicateFiller} against it, so a mismatch surfaces as
 * an overflow rather than a byte count we thought to encode. {@link #castFromTimestampBufferIsLargeEnoughToFill}
 * is the ENG-21561 shape; it's masked while {@link #castWideningBufferIsLargeEnoughToFill}'s bug is also
 * present, since the wider timestamp source then over-allocates by more than the missing precision byte.
 * {@link #dayWeekFamilyValuesBufferIsLargeEnoughToFill} is the ENG-22836 shape.
 */
class PredicateBufferSizeConsistencyTest
{
    /**
     * date(ts) BETWEEN X AND Y over a TIMESTAMP source cast to DATE - the ENG-21561 shape.
     */
    @Test
    public void castFromTimestampBufferIsLargeEnoughToFill()
    {
        assertSizedBufferFillsWithoutOverflow(
                TimestampType.createTimestampType(6),
                Domain.create(ValueSet.ofRanges(Range.range(DateType.DATE, 19_723L, true, 19_724L, true)), false),
                RecTypeCode.REC_TYPE_DATE);
    }

    /**
     * CAST(int_col AS bigint) BETWEEN X AND Y - a widening numeric CAST (4-byte source, 8-byte target).
     */
    @Test
    public void castWideningBufferIsLargeEnoughToFill()
    {
        assertSizedBufferFillsWithoutOverflow(
                IntegerType.INTEGER,
                Domain.create(ValueSet.ofRanges(Range.range(BigintType.BIGINT, 1L, true, 5L, true)), false),
                RecTypeCode.REC_TYPE_BIGINT);
    }

    /**
     * day_of_year(date_col) IN (...) - the ENG-22836 shape. Like a CAST, the DAY/WEEK family (day(),
     * day_of_week(), day_of_year(), week(), year_of_week()) always returns bigint (8-byte), so the
     * predicate buffer must be sized off that result type rather than the 4-byte DATE source column.
     */
    @Test
    public void dayWeekFamilyValuesBufferIsLargeEnoughToFill()
    {
        for (FunctionType functionType : List.of(
                FunctionType.FUNCTION_TYPE_DAY,
                FunctionType.FUNCTION_TYPE_DAY_OF_WEEK,
                FunctionType.FUNCTION_TYPE_DAY_OF_YEAR,
                FunctionType.FUNCTION_TYPE_WEEK,
                FunctionType.FUNCTION_TYPE_YEAR_OF_WEEK)) {
            assertValuesBufferFillsWithoutOverflow(functionType);
        }
    }

    private static void assertValuesBufferFillsWithoutOverflow(FunctionType functionType)
    {
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.equal(BigintType.BIGINT, 1L), Range.equal(BigintType.BIGINT, 5L), Range.equal(BigintType.BIGINT, 9L)),
                false);
        NativeExpression nativeExpression = new NativeExpression(
                PredicateType.PREDICATE_TYPE_VALUES,
                functionType,
                domain,
                false,
                true,
                List.of(),
                TransformFunction.NONE);
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        int sourceRecTypeLength = TypeUtils.getTypeLength(DateType.DATE, storageEngineConstants.getVarcharMaxLen());
        int functionTargetRecTypeLength = TypeUtils.getTypeLength(domain.getType(), storageEngineConstants.getVarcharMaxLen());
        PredicateData predicateData = PredicateUtil.calcPredicateData(
                nativeExpression,
                sourceRecTypeLength,
                false,
                DateType.DATE,
                functionTargetRecTypeLength);

        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.createBuffView(any())).thenAnswer(invocation -> ((ByteBuffer) invocation.getArgument(0)).duplicate());
        ValuesPredicateFiller filler = new ValuesPredicateFiller(bufferAllocator);

        ByteBuffer buffer = ByteBuffer.allocate(predicateData.getPredicateSize());
        assertThatCode(() -> filler.fillPredicate(domain, buffer, predicateData))
                .doesNotThrowAnyException();
    }

    private static void assertSizedBufferFillsWithoutOverflow(Type sourceType, Domain domain, RecTypeCode castTargetRecTypeCode)
    {
        NativeExpression nativeExpression = new NativeExpression(
                PredicateType.PREDICATE_TYPE_RANGES,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(castTargetRecTypeCode.ordinal()),
                TransformFunction.NONE);
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        int sourceRecTypeLength = TypeUtils.getTypeLength(sourceType, storageEngineConstants.getVarcharMaxLen());
        int castTargetRecTypeLength = TypeUtils.getTypeLength(domain.getType(), storageEngineConstants.getVarcharMaxLen());
        PredicateData predicateData = PredicateUtil.calcPredicateData(
                nativeExpression,
                sourceRecTypeLength,
                false,
                sourceType,
                castTargetRecTypeLength);

        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.createBuffView(any())).thenAnswer(invocation -> ((ByteBuffer) invocation.getArgument(0)).duplicate());
        RangesPredicateFiller filler = new RangesPredicateFiller(bufferAllocator, storageEngineConstants);

        ByteBuffer buffer = ByteBuffer.allocate(predicateData.getPredicateSize());
        assertThatCode(() -> filler.fillPredicate(domain, buffer, predicateData))
                .doesNotThrowAnyException();
    }
}
