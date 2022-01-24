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
package io.trino.plugin.objectstore.functions.tablechanges;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.objectstore.ForDelta;
import io.trino.plugin.objectstore.ForIceberg;
import io.trino.plugin.objectstore.ObjectStoreSessionProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.function.table.ConnectorTableFunction;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProvider
        implements Provider<ConnectorTableFunction>
{
    private final Connector icebergConnector;
    private final Connector deltaConnector;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public TableChangesFunctionProvider(@ForIceberg Connector icebergConnector, @ForDelta Connector deltaConnector, ObjectStoreSessionProperties sessionProperties)
    {
        this.icebergConnector = requireNonNull(icebergConnector, "icebergConnector is null");
        this.deltaConnector = requireNonNull(deltaConnector, "deltaConnector is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(
                new TableChangesFunction(icebergConnector, deltaConnector, sessionProperties),
                getClass().getClassLoader());
    }
}
