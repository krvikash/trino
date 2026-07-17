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
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.server.ServerConfig;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.tracing.ForTracing;
import io.trino.tracing.TracingSubstitutionMetadata;

import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
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

        configBinder(binder).bindConfig(MaterializedViewSubstitutionConfig.class);
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(MaterializedViewSubstitutionSessionProperties.class);
        binder.bind(NoopMaterializationService.class).in(Scopes.SINGLETON);

        // Materialization DDL only runs on the coordinator and the real service depends on coordinator-only
        // planner components, so the feature is wired solely there. Whether it is actually enabled is decided
        // at injection time inside CoordinatorMaterializationModule (see below), not by the node role.
        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            install(new CoordinatorMaterializationModule());
        }
        else {
            newOptionalBinder(binder, MaterializationIndex.class);
            binder.bind(MaterializationService.class).to(NoopMaterializationService.class).in(Scopes.SINGLETON);
        }
    }

    /**
     * We decide to enable or not substitution at injection time (as oposed to guice setup) using {@code @Provider} methods below
     * becuase only then config default values updated using {@code bindConfigDefaults()} are available.
     * SEP will configure substitution using {@code bindConfigDefaults()} based on materialization metastore (part of the portal) configuration.
     *
     * <p>
     * The beans are bound <b>without</b> {@code SINGLETON} scope on purpose: Trino builds the injector in
     * {@code Stage.PRODUCTION}, where singletons are created eagerly. Eager construction would start the index refresh thread and,
     * worse, fail {@code MaterializationIrExtractor}'s construction as it requires substitution optimizer to be present.
     */
    private static class CoordinatorMaterializationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            jsonCodecBinder(binder).bindJsonCodec(Output.class);
            newOptionalBinder(binder, RawMaterializationMetastore.class)
                    .setDefault().to(InMemoryRawMaterializationMetastore.class).in(Scopes.SINGLETON);
            binder.bind(VersionAwareMaterializationMetastore.class);
            binder.bind(MaterializationIndex.class);
            binder.bind(MaterializationIrExtractor.class);
            binder.bind(DefaultMaterializationService.class);
            // Eagerly created; it pulls Optional<MaterializationIndex>, which builds and JMX-exports the index
            // when enabled and stays empty (building nothing) when disabled.
            binder.bind(MaterializationIndexMBeanExporter.class).in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        Optional<MaterializationIndex> materializationIndex(
                MaterializedViewSubstitutionConfig config,
                Provider<MaterializationIndex> materializationIndex)
        {
            return config.isMaterializedViewSubstitutionSupportEnabled() ? Optional.of(materializationIndex.get()) : Optional.empty();
        }

        // Not @Singleton on purpose: a singleton provider is invoked eagerly in Stage.PRODUCTION and would throw
        // while disabled. Left unscoped, it is resolved only when DefaultMaterializationService (built only when
        // enabled) injects it, and it returns the same shared index the read path uses.
        @Provides
        MaterializationMetastore materializationMetastore(Optional<MaterializationIndex> materializationIndex)
        {
            return materializationIndex.orElseThrow(() -> new IllegalStateException("Materialization metastore requested while substitution is disabled"));
        }

        @Provides
        @Singleton
        MaterializationService materializationService(
                MaterializedViewSubstitutionConfig config,
                Provider<DefaultMaterializationService> defaultMaterializationService,
                NoopMaterializationService noopMaterializationService)
        {
            return config.isMaterializedViewSubstitutionSupportEnabled() ? defaultMaterializationService.get() : noopMaterializationService;
        }
    }
}
