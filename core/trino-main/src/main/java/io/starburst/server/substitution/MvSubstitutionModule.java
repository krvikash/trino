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
import io.trino.FeaturesConfig;
import io.trino.server.ServerConfig;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.tracing.ForTracing;
import io.trino.tracing.TracingSubstitutionMetadata;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

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

        boolean featureEnabled = buildConfigObject(FeaturesConfig.class).isMaterializedViewSubstitutionSupportEnabled();
        if (!featureEnabled) {
            binder.bind(MaterializationService.class).to(NoopMaterializationService.class).in(Scopes.SINGLETON);
            return;
        }

        if (!buildConfigObject(ServerConfig.class).isCoordinator()) {
            binder.bind(MaterializationService.class).toProvider(() -> {
                throw new UnsupportedOperationException("Materialization Service is not supported in a worker node");
            });
            return;
        }

        binder.bind(InMemoryRawMaterializationMetastore.class).in(Scopes.SINGLETON);
        binder.bind(RawMaterializationMetastore.class).to(InMemoryRawMaterializationMetastore.class);
        binder.bind(VersionAwareMaterializationMetastore.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationIndex.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationMetastore.class).to(MaterializationIndex.class);
        binder.bind(MaterializationService.class).to(DefaultMaterializationService.class).in(Scopes.SINGLETON);
        binder.bind(MaterializationIrExtractor.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(Output.class);
    }
}
