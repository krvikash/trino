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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.arrow.ArrowVectorConverter;

import static com.starburstdata.trino.plugin.snowflake.parallel.writer.DecimalValues.rescale;
import static java.lang.Math.max;

public class LongDecimalValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final int decimalDigits;
    private final Type type;
    private final String columnName;

    public LongDecimalValueWriter(ArrowVectorConverter converter, int decimalDigits, Type type, String columnName)
    {
        this.converter = converter;
        this.decimalDigits = decimalDigits;
        this.type = type;
        this.columnName = columnName;
    }

    @Override
    public void write(BlockBuilder output, int fromPosition, int positionCount)
            throws SFException
    {
        for (int row = fromPosition; row < fromPosition + positionCount; row++) {
            if (converter.isNull(row)) {
                output.appendNull();
            }
            else {
                type.writeObject(output, Decimals.valueOf(rescale(converter.toBigDecimal(row), max(decimalDigits, 0), columnName)));
            }
        }
    }
}
