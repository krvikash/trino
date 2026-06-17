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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.AbstractTypedJacksonModule;
import io.trino.metadata.HandleResolver;
import io.trino.plugin.base.ForwardingConnector;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.testing.PlanTester;
import io.trino.type.InternalTypeManager;
import io.trino.type.TypeDeserializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.connector.CatalogServiceProviderModule.createSubstitutionMetadata;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestUtils
{
    private TestUtils() {}

    public static final String CATALOG = "mock";
    public static final String SCHEMA = "default";
    static final ConnectorIdVersion SUPPORTED_TABLE_ID_VERSION = new ConnectorIdVersion("TestTableId", 1);
    static final ConnectorIdVersion SUPPORTED_COLUMN_ID_VERSION = new ConnectorIdVersion("TestColumnId", 1);

    /**
     * A fresh version-aware metastore over an empty in-memory raw store. Writes made directly on the returned
     * instance stand in for changes another cluster makes against a shared metastore.
     */
    public static VersionAwareMaterializationMetastore versionAwareMetastore()
    {
        return versionAwareMetastore(() -> {});
    }

    /**
     * As {@link #versionAwareMetastore()}, but runs {@code afterListMaterializations} at the end of every
     * {@code listMaterializations} call. Tests use this to inject a write-through that lands while a refresh is
     * reading the underlying metastore.
     */
    public static VersionAwareMaterializationMetastore versionAwareMetastore(Runnable afterListMaterializations)
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build());
        planTester.createCatalog(CATALOG, substitutionConnectorFactory(), ImmutableMap.of());

        SubstitutionMetadata substitutionMetadata = new SubstitutionMetadataManager(
                createSubstitutionMetadata((ConnectorServicesProvider) planTester.getCatalogManager()));
        TypeManager typeManager = InternalTypeManager.TESTING_TYPE_MANAGER;
        // The Output codec must round-trip the connector-specific ConnectorTableId / ConnectorColumnId, which
        // are persisted polymorphically via the handle resolver (mirrors the server's HandleJsonModule wiring).
        HandleResolver handleResolver = new HandleResolver();
        JsonCodec<Output> irJsonCodec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager)))
                .withModules(ImmutableSet.of(
                        new AbstractTypedJacksonModule<>(ConnectorTableId.class, handleResolver::getId, handleResolver::getHandleClass) {},
                        new AbstractTypedJacksonModule<>(ConnectorColumnId.class, handleResolver::getId, handleResolver::getHandleClass) {}))
                .get())
                .jsonCodec(Output.class);
        return new VersionAwareMaterializationMetastore(
                new InMemoryRawMaterializationMetastore(),
                irJsonCodec,
                substitutionMetadata,
                planTester.getCatalogManager())
        {
            @Override
            public List<MaterializationDefinition> listMaterializations()
            {
                List<MaterializationDefinition> result = super.listMaterializations();
                afterListMaterializations.run();
                return result;
            }
        };
    }

    public static MaterializationDefinition materialization(String name, String sourceTable)
    {
        return materialization(name, new SupportedTableId(sourceTable), new TestColumnId("name"));
    }

    public static MaterializationDefinition materialization(String name, ConnectorTableId sourceTable, ConnectorColumnId sourceColumn)
    {
        return materialization(name, simpleTableScan(sourceTable, sourceColumn));
    }

    public static MaterializationDefinition materialization(String name, Output computation)
    {
        return new MaterializationDefinition(
                computation,
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, name + "_storage", "v1")),
                new MaterializationSource.MaterializedViewSource(mvName(name)),
                Instant.now(),
                Optional.empty());
    }

    public static Output simpleTableScan(ConnectorTableId sourceTable, ConnectorColumnId sourceColumn)
    {
        Symbol nameSymbol = new Symbol(VARCHAR, "name");
        return new Output(
                ImmutableList.of("name"),
                ImmutableList.of(nameSymbol),
                new TableScan(
                        new TableId(new CatalogName(CATALOG), sourceTable),
                        ImmutableMap.of(sourceColumn, nameSymbol)));
    }

    public static CatalogSchemaTableName mvName(String name)
    {
        return new CatalogSchemaTableName(CATALOG, SCHEMA, name);
    }

    private static ConnectorFactory substitutionConnectorFactory()
    {
        MockConnectorFactory delegate = MockConnectorFactory.builder().build();
        return new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "mock_substitution";
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                Connector connector = delegate.create(catalogName, config, context);
                return new ForwardingConnector()
                {
                    @Override
                    protected Connector delegate()
                    {
                        return connector;
                    }

                    @Override
                    public ConnectorSubstitutionMetadata getSubstitutionMetadata()
                    {
                        return new TestSubstitutionMetadata();
                    }
                };
            }
        };
    }

    private static class TestSubstitutionMetadata
            implements ConnectorSubstitutionMetadata
    {
        @Override
        public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
        {
            return false;
        }

        @Override
        public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
        {
            return Optional.empty();
        }

        @Override
        public Set<ConnectorIdVersion> tableIdVersions()
        {
            return ImmutableSet.of(SUPPORTED_TABLE_ID_VERSION);
        }

        @Override
        public Set<ConnectorIdVersion> columnIdVersions()
        {
            return ImmutableSet.of(SUPPORTED_COLUMN_ID_VERSION);
        }
    }

    public record SupportedTableId(String tableName)
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
            return SUPPORTED_TABLE_ID_VERSION;
        }
    }

    public record UnsupportedTableId(String tableName)
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
            return new ConnectorIdVersion("TestTableId", 2);
        }
    }

    public record TestColumnId(String columnName)
            implements ConnectorColumnId
    {
        @Override
        public ConnectorIdVersion version()
        {
            return SUPPORTED_COLUMN_ID_VERSION;
        }
    }
}
