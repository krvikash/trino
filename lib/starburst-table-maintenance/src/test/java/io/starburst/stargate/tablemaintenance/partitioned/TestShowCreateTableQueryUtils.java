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
package io.starburst.stargate.tablemaintenance.partitioned;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestShowCreateTableQueryUtils
{
    @Test
    public void testParseFormatVersionV1()
    {
        String output =
                """
                CREATE TABLE test_table (col1 varchar)
                WITH (
                  type = 'ICEBERG',
                  format_version = 1
                )
                """;
        assertThat(ShowCreateTableQueryUtils.parseFormatVersion(output)).hasValue(1);
    }

    @Test
    public void testParseFormatVersionV2()
    {
        String output =
                """
                CREATE TABLE test_table (col1 varchar)
                WITH (
                  type = 'ICEBERG',
                  format_version = 2,
                  format = 'PARQUET'
                )
                """;
        assertThat(ShowCreateTableQueryUtils.parseFormatVersion(output)).hasValue(2);
    }

    @Test
    public void testParseFormatVersionV3()
    {
        String output = "CREATE TABLE t (c int) WITH (format_version = 3)";
        assertThat(ShowCreateTableQueryUtils.parseFormatVersion(output)).hasValue(3);
    }

    @Test
    public void testParseFormatVersionMissing()
    {
        String output =
                """
                CREATE TABLE test_table (col1 varchar)
                WITH (
                  type = 'ICEBERG',
                  format = 'PARQUET'
                )
                """;
        assertThat(ShowCreateTableQueryUtils.parseFormatVersion(output)).isEmpty();
    }

    @Test
    public void testParseFormatVersionWithSpaces()
    {
        String output = "CREATE TABLE t (c int) WITH (format_version  =  2)";
        assertThat(ShowCreateTableQueryUtils.parseFormatVersion(output)).hasValue(2);
    }
}
