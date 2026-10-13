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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableList;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;

final class IrFixtures
{
    private IrFixtures() {}

    static Output outputOverScan(Symbol... columns)
    {
        return new Output(
                stream(columns).map(Symbol::name).collect(toImmutableList()),
                ImmutableList.copyOf(columns),
                tableScan(columns));
    }

    static TableScan tableScan(Symbol... columns)
    {
        return new TableScan(
                new TableId(new CatalogName("catalog"), new FixtureTableId("table")),
                stream(columns).collect(toImmutableMap(symbol -> new FixtureColumnId(symbol.name()), symbol -> symbol)));
    }

    static Reference reference(Type type, String name)
    {
        return new Reference(new Symbol(type, name));
    }

    static Constant constant(Type type, @Nullable Object nativeValue)
    {
        return new Constant(type, ConstantValues.encode(type, nativeValue).orElseThrow());
    }

    static BoundSignature builtinSignature(String functionName, Type returnType, Type... argumentTypes)
    {
        return new BoundSignature(new CatalogSchemaFunctionName("system", "builtin", functionName), returnType, ImmutableList.copyOf(argumentTypes));
    }

    record FixtureTableId(String tableName)
            implements ConnectorTableId
    {
        @Override
        public long hash()
        {
            return tableName.hashCode();
        }

        @Override
        public ConnectorIdVersion version()
        {
            return new ConnectorIdVersion("FixtureTableId", 1);
        }
    }

    record FixtureColumnId(String columnName)
            implements ConnectorColumnId
    {
        @Override
        public ConnectorIdVersion version()
        {
            return new ConnectorIdVersion("FixtureColumnId", 1);
        }
    }
}
