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
package io.trino.plugin.objectstore;

import com.google.common.collect.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.objectstore.functions.tablechanges.TableChangesFunctionProvider;
import io.trino.plugin.objectstore.functions.unload.OjbectStoreUnloadFunctionProvider;
import io.trino.plugin.objectstore.procedure.ObjectStoreFlushMetadataCache;
import io.trino.plugin.objectstore.procedure.ObjectStoreRegisterTableProcedure;
import io.trino.plugin.objectstore.procedure.ObjectStoreUnregisterTableProcedure;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ObjectStoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DelegateConnectors.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreConnector.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStorePageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStorePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreMaterializedViewProperties.class).in(Scopes.SINGLETON);
        binder.bind(ObjectStoreSessionProperties.class).in(Scopes.SINGLETON);

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(ObjectStoreRegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(ObjectStoreUnregisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(ObjectStoreFlushMetadataCache.class).in(Scopes.SINGLETON);

        Multibinder<ConnectorTableFunction> functions = newSetBinder(binder, ConnectorTableFunction.class);
        functions.addBinding().toProvider(TableChangesFunctionProvider.class).in(Scopes.SINGLETON);
        functions.addBinding().toProvider(OjbectStoreUnloadFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(ObjectStoreFunctionProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ObjectStoreConfig.class);
        configBinder(binder).bindConfig(HiveConfig.class);

        binder.bind(FeatureExposures.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(new TypeLiteral<Optional<Table<TableType, String, FeatureExposure>>>() {}, AdditionalSessionExposures.class))
                .setDefault()
                .toInstance(Optional.empty());
        newOptionalBinder(binder, Key.get(new TypeLiteral<Optional<Table<TableType, String, FeatureExposure>>>() {}, AdditionalProcedureExposures.class))
                .setDefault()
                .toInstance(Optional.empty());
        newOptionalBinder(binder, Key.get(new TypeLiteral<Optional<Table<TableType, SchemaTableName, FeatureExposure>>>() {}, AdditionalSystemTableExposures.class))
                .setDefault()
                .toInstance(Optional.empty());
    }
}
