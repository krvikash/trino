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
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.metadata.AbstractTypedJacksonModule;
import io.trino.metadata.HandleResolver;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.type.InternalTypeManager;
import io.trino.type.TypeDeserializer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;

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
        return versionAwareMetastore(new InMemoryRawMaterializationMetastore(), () -> {});
    }

    /**
     * As {@link #versionAwareMetastore()}, but over the given raw store, so a test can plant rows that a
     * different build of the connectors wrote.
     */
    public static VersionAwareMaterializationMetastore versionAwareMetastore(RawMaterializationMetastore rawMetastore)
    {
        return versionAwareMetastore(rawMetastore, () -> {});
    }

    /**
     * As {@link #versionAwareMetastore()}, but runs {@code afterListMaterializations} at the end of every
     * {@code listMaterializations} call. Tests use this to inject a write-through that lands while a refresh is
     * reading the underlying metastore.
     */
    public static VersionAwareMaterializationMetastore versionAwareMetastore(Runnable afterListMaterializations)
    {
        return versionAwareMetastore(new InMemoryRawMaterializationMetastore(), afterListMaterializations);
    }

    private static VersionAwareMaterializationMetastore versionAwareMetastore(RawMaterializationMetastore rawMetastore, Runnable afterListMaterializations)
    {
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
        return new VersionAwareMaterializationMetastore(rawMetastore, irJsonCodec)
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

    /**
     * Copies {@code definition} under a new name with {@code irJson} as its IR; version stamps are unchanged.
     */
    public static RawMaterializationDefinition copyWithIrJson(RawMaterializationDefinition definition, String name, String irJson)
    {
        return new RawMaterializationDefinition(
                definition.irVersions(),
                definition.catalogIrVersions(),
                irJson,
                definition.storageTableId(),
                new MaterializationSource.MaterializedViewSource(mvName(name)),
                definition.lastKnownFreshTime(),
                definition.gracePeriod());
    }

    public static CatalogSchemaTableName mvName(String name)
    {
        return new CatalogSchemaTableName(CATALOG, SCHEMA, name);
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
