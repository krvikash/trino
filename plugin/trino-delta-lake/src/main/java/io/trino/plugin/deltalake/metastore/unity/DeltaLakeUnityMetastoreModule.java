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
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoOptional;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.filesystem.s3.ForS3ClientCaching;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemLoader;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.filesystem.s3.S3SecurityMappingProvider;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentialsProvider;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.unity.dynamic.DynamicS3CredentialConfig;
import io.trino.plugin.deltalake.metastore.unity.dynamic.DynamicUnityMetastoreConfig;
import io.trino.plugin.deltalake.metastore.unity.dynamic.ExtraCredentialsS3SecurityMappingProvider;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.reader.UnityTransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.deltalake.transactionlog.writer.UnityTransactionLogWriterFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.metastore.unity.PersonalAccessTokenProvider;
import io.trino.plugin.hive.metastore.unity.SupportedUnityTableFormatsProvider;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastoreConfig;
import io.trino.plugin.hive.metastore.unity.UnityMetastoreProxyConfig;
import io.trino.plugin.hive.metastore.unity.UnityTokenProvider;
import io.trino.spi.TrinoException;

import static com.databricks.sdk.service.catalog.DataSourceFormat.AVRO;
import static com.databricks.sdk.service.catalog.DataSourceFormat.CSV;
import static com.databricks.sdk.service.catalog.DataSourceFormat.DELTA;
import static com.databricks.sdk.service.catalog.DataSourceFormat.JSON;
import static com.databricks.sdk.service.catalog.DataSourceFormat.ORC;
import static com.databricks.sdk.service.catalog.DataSourceFormat.PARQUET;
import static com.databricks.sdk.service.catalog.DataSourceFormat.TEXT;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.multibindings.ProvidesIntoOptional.Type.ACTUAL;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class DeltaLakeUnityMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // TODO https://starburstdata.atlassian.net/browse/CONNECT-602
        if (buildConfigObject(DeltaLakeConfig.class).isRegisterTableProcedureEnabled()) {
            binder.addError(new TrinoException(NOT_SUPPORTED, "Register procedure is not supported for Unity"));
        }

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

        if (buildConfigObject(DeltaLakeConfig.class).getHiveCatalogName().isPresent()) {
            binder.bind(SupportedUnityTableFormatsProvider.class)
                    .toInstance(() -> ImmutableSet.of(DELTA, PARQUET, AVRO, ORC, CSV, JSON, TEXT));
        }
        else {
            binder.bind(SupportedUnityTableFormatsProvider.class)
                    .toInstance(() -> ImmutableSet.of(DELTA));
        }

        newOptionalBinder(binder, TransactionLogReaderFactory.class)
                .setBinding().to(UnityTransactionLogReaderFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TransactionLogWriterFactory.class)
                .setBinding().to(UnityTransactionLogWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeUnityMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        // Databricks denied sharing the exact value as its undocumented but confirmed that it's greater than 512K when given Glue's reference.
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(512000);

        if (buildConfigObject(DeltaLakeConfig.class).isDynamicConfigurationPassthroughEnabled()) {
            install(new DynamicConfigurationPassthroughModule());
        }
        else {
            install(new StaticConfigurationModule());
        }
    }

    private static class DynamicConfigurationPassthroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(DynamicUnityMetastoreConfig.class);
            configBinder(binder).bindConfig(DynamicS3CredentialConfig.class);

            DynamicUnityMetastoreConfig credentialConfig = buildConfigObject(DynamicUnityMetastoreConfig.class);
            binder.bind(Key.get(boolean.class, CatalogManagedTableEnabled.class)).toInstance(credentialConfig.isCatalogManagedTableEnabled());

            // Both providers handle the vended/direct decision at runtime based on extraCredentials.
            // DynamicUnityDeltaLakeTableCredentialsProvider returns empty when the vended-credentials flag is absent or false.
            // ExtraCredentialsS3SecurityMappingProvider returns empty when the S3 access key is absent (vended path:
            // the identity already carries the Unity-issued credentials, no additional mapping is needed).
            newOptionalBinder(binder, DeltaLakeTableCredentialsProvider.class).setBinding().to(DynamicUnityDeltaLakeTableCredentialsProvider.class).in(Scopes.SINGLETON);
            binder.bind(S3SecurityMappingProvider.class).to(ExtraCredentialsS3SecurityMappingProvider.class).in(SINGLETON);
            configBinder(binder).bindConfig(S3FileSystemConfig.class);
            binder.bind(S3FileSystemStats.class).in(SINGLETON);
            binder.bind(boolean.class).annotatedWith(ForS3ClientCaching.class).toInstance(false);
            binder.bind(S3FileSystemLoader.class).in(SINGLETON);

            binder.bind(DeltaLakeDynamicUnityMetastoreFactory.class).in(Scopes.SINGLETON);
            newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                    .setDefault()
                    .to(DeltaLakeDynamicUnityMetastoreFactory.class)
                    .in(Scopes.SINGLETON);
        }

        @ProvidesIntoOptional(ACTUAL)
        @Singleton
        @FileSystemS3
        public TrinoFileSystemFactory createS3FileSystemFactory(S3FileSystemLoader loader)
        {
            return new SwitchingFileSystemFactory(loader);
        }
    }

    private static class StaticConfigurationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(UnityMetastoreConfig.class);
            newOptionalBinder(binder, UnityMetastoreProxyConfig.class);
            UnityMetastoreConfig unityMetastoreConfig = buildConfigObject(UnityMetastoreConfig.class);
            if (unityMetastoreConfig.isProxyEnabled()) {
                configBinder(binder).bindConfig(UnityMetastoreProxyConfig.class);
            }

            binder.bind(Key.get(boolean.class, CatalogManagedTableEnabled.class)).toInstance(unityMetastoreConfig.isCatalogManagedTableEnabled());

            newOptionalBinder(binder, UnityTokenProvider.class)
                    .setDefault()
                    .to(PersonalAccessTokenProvider.class)
                    .in(Scopes.SINGLETON);

            binder.bind(UnityHiveMetastoreFactory.class).in(Scopes.SINGLETON);
            newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                    .setDefault()
                    .to(UnityHiveMetastoreFactory.class)
                    .in(Scopes.SINGLETON);

            if (unityMetastoreConfig.isVendedCredentialsEnabled()) {
                newOptionalBinder(binder, DeltaLakeTableCredentialsProvider.class).setBinding().to(UnityDeltaLakeTableCredentialsProvider.class).in(Scopes.SINGLETON);
                newOptionalBinder(binder, DeltaLakeFileSystemFactory.class).setBinding().to(UnityVendedCredentialsFileSystemFactory.class).in(Scopes.SINGLETON);
            }
        }
    }
}
