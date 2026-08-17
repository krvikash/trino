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
import io.trino.spi.type.Type;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.arrow.ArrowVectorConverter;

import static com.google.common.base.Verify.verify;

public class TimeValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final Type type;

    public TimeValueWriter(ArrowVectorConverter converter, Type type)
    {
        this.converter = converter;
        this.type = type;
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
                verify(converter instanceof StarburstTimeConverter, "converter should be StarburstTimeConverter type");
                type.writeLong(output, ((StarburstTimeConverter) converter).toTrinoTime(row));
            }
        }
    }
}
