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
package io.trino.plugin.warp.it.nativeengine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;

public class TestHiveWarpNativeFunctionPredicates
        extends BaseWarpNativeFunctionPredicateTest
{
    @Override
    protected Map<String, String> proxiedConnectorConfig()
    {
        return ImmutableMap.of(PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME);
    }

    @Override
    protected int timestampPrecision()
    {
        return 3;
    }

    @Override
    protected String tableProperties()
    {
        return "";
    }

    // Hive accepts the small integer and bounded varchar types Iceberg rejects.
    @Override
    protected List<Column> extraColumns()
    {
        return ImmutableList.of(
                new Column("t8", "tinyint", ImmutableList.of(
                        "TINYINT '0'",
                        "TINYINT '1'",
                        "TINYINT '-1'",
                        "TINYINT '127'",
                        "TINYINT '-128'",
                        "TINYINT '42'",
                        "TINYINT '7'",
                        "TINYINT '8'",
                        "TINYINT '9'",
                        "TINYINT '10'",
                        "TINYINT '11'",
                        "TINYINT '12'",
                        "TINYINT '13'",
                        "TINYINT '14'",
                        "TINYINT '15'")),
                new Column("s16", "smallint", ImmutableList.of(
                        "SMALLINT '0'",
                        "SMALLINT '1'",
                        "SMALLINT '-1'",
                        "SMALLINT '32767'",
                        "SMALLINT '-32768'",
                        "SMALLINT '42'",
                        "SMALLINT '7'",
                        "SMALLINT '8'",
                        "SMALLINT '9'",
                        "SMALLINT '10'",
                        "SMALLINT '11'",
                        "SMALLINT '12'",
                        "SMALLINT '13'",
                        "SMALLINT '14'",
                        "SMALLINT '15'")),
                new Column("v20", "varchar(20)", ImmutableList.of(
                        "''",
                        "'a'",
                        "'A'",
                        "'MacBook Pro'",
                        "'macbook pro'",
                        "'MACBOOK PRO'",
                        "'Äpfel'",
                        "'äpfel'",
                        "'straße'",
                        "'STRASSE'",
                        "'STRAßE'",
                        "' spaced '",
                        "'a,b,c'",
                        "'{\"a\":\"x\",\"n\":1}'",
                        "'Mac'")));
    }

    @Override
    protected List<String> extraPredicates()
    {
        return ImmutableList.of(
                "CAST(t8 AS bigint) = 42",
                "CAST(t8 AS bigint) IN (-128, 127)",
                "CAST(t8 AS integer) < 0",
                "CAST(t8 AS smallint) = -1",
                "CAST(t8 AS double) = 127.0",
                "CAST(t8 AS real) = REAL '-128.0'",
                "CAST(s16 AS bigint) = 42",
                "CAST(s16 AS bigint) IN (-32768, 32767)",
                "CAST(s16 AS integer) BETWEEN 7 AND 9",
                "CAST(s16 AS double) = 32767.0",
                "CAST(s16 AS real) < REAL '0.0'",
                "CAST(t8 AS varchar) = '-128'",
                "CAST(s16 AS varchar) IN ('0', '32767')",
                "CAST(v20 AS varchar(30)) = 'MacBook Pro'",
                "CAST(v20 AS varchar(20)) IN ('a', 'straße')",
                "CAST(v20 AS varchar) = 'Mac'",
                "lower(v20) = 'macbook pro'",
                "upper(v20) = 'MACBOOK PRO'");
    }
}
