/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.client.ForMaterializationMetastoreClient;
import io.starburst.materialization.metastore.client.HttpMaterializationMetastore;
import io.starburst.materialization.metastore.client.MaterializationMetastoreClientConfig;
import io.trino.FeaturesConfig;
import io.trino.server.ServerConfig;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.tracing.ForTracing;
import io.trino.tracing.TracingSubstitutionMetadata;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MvSubstitutionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SubstitutionMetadata.class).annotatedWith(ForTracing.class).to(SubstitutionMetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(SubstitutionMetadata.class).to(TracingSubstitutionMetadata.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableId.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorColumnId.class);
        newOptionalBinder(binder, MaterializationIndex.class);

        FeaturesConfig featuresConfig = buildConfigObject(FeaturesConfig.class);
        if (!featuresConfig.isMaterializedViewSubstitutionSupportEnabled()) {
            binder.bind(MaterializationService.class).to(NoopMaterializationService.class).in(Scopes.SINGLETON);
            return;
        }

        if (!buildConfigObject(ServerConfig.class).isCoordinator()) {
            binder.bind(MaterializationService.class).toProvider(() -> {
                throw new UnsupportedOperationException("Materialization Service is not supported in a worker node");
            });
            return;
        }

        switch (featuresConfig.getMaterializationMetastoreType()) {
            case IN_MEMORY -> {
                binder.bind(RawMaterializationMetastore.class).to(InMemoryRawMaterializationMetastore.class).in(Scopes.SINGLETON);
            }
            case REST -> {
                configBinder(binder).bindConfig(MaterializationMetastoreClientConfig.class);
                httpClientBinder(binder).bindHttpClient("materialization-metastore", ForMaterializationMetastoreClient.class);
                binder.bind(RawMaterializationMetastore.class).to(HttpMaterializationMetastore.class).in(Scopes.SINGLETON);
            }
        }
        binder.bind(VersionAwareMaterializationMetastore.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationIndex.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationMetastore.class).to(MaterializationIndex.class);
        newExporter(binder).export(MaterializationIndex.class).withGeneratedName();
        binder.bind(MaterializationService.class).to(DefaultMaterializationService.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationIrExtractor.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(Output.class);
    }
}
