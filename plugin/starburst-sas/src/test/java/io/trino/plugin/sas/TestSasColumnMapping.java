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
package io.trino.plugin.sas;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestSasColumnMapping
{
    @Test
    void testColumnMapping()
    {
        assertSasColumn(null, String.class, VarcharType.createVarcharType(5));
        assertSasColumn(null, Number.class, DoubleType.DOUBLE);
        assertSasColumn("TIME", Number.class, TimestampType.TIMESTAMP_MILLIS);
        assertSasColumn("HOUR", Number.class, IntegerType.INTEGER);
        assertSasColumn("MMSS", Number.class, IntegerType.INTEGER);
        assertSasColumn("PERCENT", Number.class, DoubleType.DOUBLE);
        assertSasColumn("HHMM", Number.class, IntegerType.INTEGER);
        assertSasColumn("E8601TM", Number.class, VarcharType.createUnboundedVarcharType());
        assertSasColumn("TIMEAMPM", Number.class, TimestampType.TIMESTAMP_MILLIS);
        assertSasColumn("E8601LS", Number.class, VarcharType.createUnboundedVarcharType());
        assertSasColumn("MMDDYY", Number.class, DateType.DATE);
        assertSasColumn("DDMMYYD", Number.class, DateType.DATE);
    }

    private static void assertSasColumn(String format, Class<?> sasClass, Type expected)
    {
        assertThat(SasColumn.getColumn(new Column(0, "colonne", "label", new ColumnFormat(format, 0, 0), sasClass, 5)).type())
                .isEqualTo(expected);
    }
}
