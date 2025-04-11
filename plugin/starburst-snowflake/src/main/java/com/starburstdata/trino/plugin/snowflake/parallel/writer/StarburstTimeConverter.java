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

import com.google.common.collect.ImmutableList;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.arrow.BigIntToTimeConverter;
import net.snowflake.client.core.arrow.IntToTimeConverter;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.BigIntVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.IntVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;

import java.util.List;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;

public sealed interface StarburstTimeConverter
        permits StarburstTimeConverter.StarburstIntToTimeConverter, StarburstTimeConverter.StarburstBigintToTimeConverter
{
    List<Integer> POWERS_OF_TEN = ImmutableList.of(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000);

    default long toTrinoTime(int index)
    {
        int powerOfTen = POWERS_OF_TEN.get(9 - getScale());
        // Snowflake SFTime implementation keeps time in nano (max supported resolution by SF)
        long nanos = getValue(index) * (long) powerOfTen;
        // Trino expects pico resolution
        return nanos * PICOSECONDS_PER_NANOSECOND;
    }

    long getValue(int index);

    int getScale();

    final class StarburstIntToTimeConverter
            extends IntToTimeConverter
            implements StarburstTimeConverter
    {
        private final IntVector intVector;

        public StarburstIntToTimeConverter(
                ValueVector fieldVector,
                int columnIndex,
                DataConversionContext context)
        {
            super(fieldVector, columnIndex, context);
            this.intVector = (IntVector) fieldVector;
        }

        @Override
        public long getValue(int index)
        {
            return intVector.getDataBuffer().getInt((long) index * IntVector.TYPE_WIDTH);
        }

        @Override
        public int getScale()
        {
            return context.getScale(columnIndex);
        }
    }

    final class StarburstBigintToTimeConverter
            extends BigIntToTimeConverter
            implements StarburstTimeConverter
    {
        private final BigIntVector bigIntVector;

        public StarburstBigintToTimeConverter(
                ValueVector fieldVector,
                int columnIndex,
                DataConversionContext context)
        {
            super(fieldVector, columnIndex, context);
            this.bigIntVector = (BigIntVector) fieldVector;
        }

        @Override
        public long getValue(int index)
        {
            return bigIntVector.getDataBuffer().getLong((long) index * BigIntVector.TYPE_WIDTH);
        }

        @Override
        public int getScale()
        {
            return context.getScale(columnIndex);
        }
    }
}
