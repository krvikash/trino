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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;

public class TestIcebergWarpNativeFunctionPredicates
        extends BaseWarpNativeFunctionPredicateTest
{
    @Override
    protected Map<String, String> proxiedConnectorConfig()
    {
        return ImmutableMap.of(
                PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME,
                "iceberg.catalog.type", "TESTING_FILE_METASTORE");
    }

    @Override
    protected int timestampPrecision()
    {
        return 6;
    }

    @Override
    protected String tableProperties()
    {
        return " WITH (format = 'PARQUET')";
    }

    @Override
    protected void addFunctions(QueryRunner queryRunner)
    {
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());
    }
}
