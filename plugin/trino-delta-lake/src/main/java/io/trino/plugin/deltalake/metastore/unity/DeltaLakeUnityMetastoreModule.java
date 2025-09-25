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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.reader.UnityTransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.deltalake.transactionlog.writer.UnityTransactionLogWriterFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.metastore.unity.SupportedUnityTableFormatsProvider;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastoreConfig;
import io.trino.spi.TrinoException;

import static com.databricks.sdk.service.catalog.DataSourceFormat.AVRO;
import static com.databricks.sdk.service.catalog.DataSourceFormat.CSV;
import static com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
import static com.databricks.sdk.service.catalog.DataSourceFormat.JSON;
import static com.databricks.sdk.service.catalog.DataSourceFormat.ORC;
import static com.databricks.sdk.service.catalog.DataSourceFormat.PARQUET;
import static com.databricks.sdk.service.catalog.DataSourceFormat.TEXT;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class DeltaLakeUnityMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(UnityMetastoreConfig.class);

        // TODO https://starburstdata.atlassian.net/browse/CONNECT-602
        if (buildConfigObject(DeltaLakeConfig.class).isRegisterTableProcedureEnabled()) {
            binder.addError(new TrinoException(NOT_SUPPORTED, "Register procedure is not supported for Unity"));
        }

        binder.bind(UnityHiveMetastoreFactory.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(UnityHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

        install(conditionalModule(
                DeltaLakeConfig.class,
                config -> config.getHiveCatalogName().isPresent(),
                hiveAndDeltaFormatsBinder -> hiveAndDeltaFormatsBinder
                        .bind(SupportedUnityTableFormatsProvider.class)
                        .toInstance(() -> ImmutableSet.of(DELTA, PARQUET, AVRO, ORC, CSV, JSON, TEXT)),
                hiveFormatsBinder -> hiveFormatsBinder
                        .bind(SupportedUnityTableFormatsProvider.class)
                        .toInstance(() -> ImmutableSet.of(DELTA))));

        newOptionalBinder(binder, TransactionLogReaderFactory.class)
                .setBinding().to(UnityTransactionLogReaderFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TransactionLogWriterFactory.class)
                .setBinding().to(UnityTransactionLogWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeUnityMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        // Databricks denied sharing the exact value as its undocumented but confirmed that it's greater than 512K when given Glue's reference.
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(512000);

        install(conditionalModule(
                UnityMetastoreConfig.class,
                UnityMetastoreConfig::isVendedCredentialsEnabled,
                _ -> newOptionalBinder(binder, VendedCredentialsProvider.class).setBinding().to(UnityVendedCredentialsProvider.class).in(Scopes.SINGLETON)));
    }
}
