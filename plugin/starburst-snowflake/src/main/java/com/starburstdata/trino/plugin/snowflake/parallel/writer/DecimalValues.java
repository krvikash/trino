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

import io.trino.spi.TrinoException;

import java.math.BigDecimal;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.math.RoundingMode.UNNECESSARY;

final class DecimalValues
{
    private DecimalValues() {}

    /**
     * Rescales a value decoded from Arrow to the scale Snowflake reported for the column, without
     * rounding. Snowflake can return a value carrying more fractional digits than its column
     * metadata declares, in which case the value does not fit the Trino type derived from that
     * metadata. The disagreement is in the remote data, so it is reported as an external error
     * naming the column and both scales, rather than letting the bare ArithmeticException from
     * BigDecimal surface as an internal error.
     */
    static BigDecimal rescale(BigDecimal value, int scale, String columnName)
    {
        try {
            return value.setScale(scale, UNNECESSARY);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(
                    JDBC_ERROR,
                    "Snowflake column %s returned a value with scale %s, exceeding the scale %s declared for the column".formatted(columnName, value.scale(), scale),
                    e);
        }
    }
}
