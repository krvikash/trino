/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel.writer;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

class TestBlockWriter
{
    private static final Long[] VALUES = {10L, null, 30L, 40L, 50L};

    @Test
    void testWritesOnlyRequestedRange()
            throws SFException
    {
        LongValueWriter writer = new LongValueWriter(new FakeLongConverter(VALUES), BIGINT);
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 3);

        // Write positions [1, 4): null, 30, 40 - the rows before and after the range must be ignored.
        writer.write(builder, 1, 3);
        Block block = builder.build();

        assertThat(block.getPositionCount()).isEqualTo(3);
        assertThat(block.isNull(0)).isTrue();
        assertThat(BIGINT.getLong(block, 1)).isEqualTo(30L);
        assertThat(BIGINT.getLong(block, 2)).isEqualTo(40L);
    }

    @Test
    void testConsecutiveSlicesConcatenate()
            throws SFException
    {
        // Emitting a record batch in slices (as the page source does when a page fills up) must yield
        // exactly the same block as writing it in one shot.
        LongValueWriter writer = new LongValueWriter(new FakeLongConverter(VALUES), BIGINT);

        BlockBuilder sliced = BIGINT.createBlockBuilder(null, VALUES.length);
        writer.write(sliced, 0, 2);
        writer.write(sliced, 2, 2);
        writer.write(sliced, 4, 1);
        Block slicedBlock = sliced.build();

        BlockBuilder whole = BIGINT.createBlockBuilder(null, VALUES.length);
        writer.write(whole, 0, VALUES.length);
        Block wholeBlock = whole.build();

        assertThat(slicedBlock.getPositionCount()).isEqualTo(VALUES.length);
        for (int position = 0; position < VALUES.length; position++) {
            assertThat(slicedBlock.isNull(position)).isEqualTo(VALUES[position] == null);
            if (VALUES[position] != null) {
                assertThat(BIGINT.getLong(slicedBlock, position)).isEqualTo(BIGINT.getLong(wholeBlock, position));
            }
        }
    }

    /**
     * Reads {@code long} values (and nulls) from a backing array by absolute row index, mirroring how
     * Snowflake's Arrow converters expose a decoded vector. Only the methods used by
     * {@link LongValueWriter} are implemented.
     */
    private record FakeLongConverter(Long[] values)
            implements ArrowVectorConverter
    {
        @Override
        public boolean isNull(int index)
        {
            return values[index] == null;
        }

        @Override
        public long toLong(int index)
        {
            return values[index];
        }

        @Override
        public void setUseSessionTimezone(boolean useSessionTimezone) {}

        @Override
        public void setSessionTimeZone(TimeZone tz) {}

        @Override
        public void setTreatNTZAsUTC(boolean isUTC) {}

        @Override
        public boolean toBoolean(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte toByte(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public short toShort(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int toInt(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double toDouble(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public float toFloat(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toBytes(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date toDate(int index, TimeZone jvmTz, boolean useDateFormat)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Time toTime(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Timestamp toTimestamp(int index, TimeZone tz)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BigDecimal toBigDecimal(int index)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object toObject(int index)
        {
            throw new UnsupportedOperationException();
        }
    }
}
