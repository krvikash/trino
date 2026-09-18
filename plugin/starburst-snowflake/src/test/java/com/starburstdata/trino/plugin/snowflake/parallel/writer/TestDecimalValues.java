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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.starburstdata.trino.plugin.snowflake.parallel.writer.DecimalValues.rescale;
import static io.trino.spi.ErrorType.EXTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDecimalValues
{
    @Test
    void testRescaleToDeclaredScale()
    {
        assertThat(rescale(new BigDecimal("1.50"), 2, "price")).isEqualTo(new BigDecimal("1.50"));
        assertThat(rescale(new BigDecimal("1.5"), 4, "price")).isEqualTo(new BigDecimal("1.5000"));
    }

    @Test
    void testValueExceedingDeclaredScaleFailsAsExternalError()
    {
        // Snowflake reported scale 2 for the column but returned a value with scale 3.
        assertThatThrownBy(() -> rescale(new BigDecimal("1.005"), 2, "price"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode().getType()).isEqualTo(EXTERNAL))
                .hasMessage("Snowflake column price returned a value with scale 3, exceeding the scale 2 declared for the column");
    }
}
