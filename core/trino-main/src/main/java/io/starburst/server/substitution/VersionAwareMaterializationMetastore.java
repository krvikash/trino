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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VersionAwareMaterializationMetastore
        implements MaterializationMetastore
{
    private static final Logger log = Logger.get(VersionAwareMaterializationMetastore.class);

    private final RawMaterializationMetastore raw;
    private final JsonCodec<Output> irJsonCodec;
    private final SubstitutionMetadata substitutionMetadata;
    private final CatalogManager catalogManager;

    @Inject
    public VersionAwareMaterializationMetastore(
            RawMaterializationMetastore raw,
            JsonCodec<Output> irJsonCodec,
            SubstitutionMetadata substitutionMetadata,
            CatalogManager catalogManager)
    {
        this.raw = requireNonNull(raw, "raw is null");
        this.irJsonCodec = requireNonNull(irJsonCodec, "irJsonCodec is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @Override
    public List<MaterializationDefinition> listMaterializations()
    {
        return raw.listMaterializations().stream()
                .map(this::toTyped)
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    @Override
    public void createOrReplace(MaterializationDefinition def)
    {
        Output typed = def.computationPlanRoot();
        Map<String, Integer> versionMap = new HashMap<>();
        Map<CatalogName, ConnectorIdVersions> catalogVersions = new HashMap<>();
        collectVersions(typed, versionMap, catalogVersions);
        String irJson = irJsonCodec.toJson(typed);
        raw.createOrReplace(new RawMaterializationDefinition(
                ImmutableMap.copyOf(versionMap),
                ImmutableMap.copyOf(catalogVersions),
                irJson,
                def.storageTableId(),
                def.source(),
                def.lastKnownFreshTime(),
                def.gracePeriod()));
    }

    @Override
    public void remove(CatalogSchemaTableName materializedViewName)
    {
        raw.remove(materializedViewName);
    }

    @Override
    public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        raw.renameIfExists(source, target, targetStorageTableId);
    }

    private Optional<MaterializationDefinition> toTyped(RawMaterializationDefinition rawDefinition)
    {
        if (!isCompatible(rawDefinition)) {
            log.info("Skipping materialization %s — stored version map %s does not match engine supported %s",
                    rawDefinition.source(),
                    rawDefinition.irVersions(),
                    Operation.SUPPORTED);
            return Optional.empty();
        }
        Output typed = irJsonCodec.fromJson(rawDefinition.computationPlanRootJson());
        return Optional.of(new MaterializationDefinition(
                typed,
                rawDefinition.storageTableId(),
                rawDefinition.source(),
                rawDefinition.lastKnownFreshTime(),
                rawDefinition.gracePeriod()));
    }

    private boolean isCompatible(RawMaterializationDefinition definition)
    {
        for (Map.Entry<String, Integer> e : definition.irVersions().entrySet()) {
            Integer expected = Operation.SUPPORTED.get(e.getKey());
            if (expected == null || !expected.equals(e.getValue())) {
                return false;
            }
        }
        for (Map.Entry<CatalogName, ConnectorIdVersions> entry : definition.catalogIrVersions().entrySet()) {
            Optional<Catalog> catalog = catalogManager.getCatalog(entry.getKey());
            if (catalog.isEmpty()) {
                return false;
            }
            CatalogHandle catalogHandle = catalog.get().getCatalogHandle();
            if (!substitutionMetadata.tableIdVersions(catalogHandle).containsAll(entry.getValue().tableIdVersions())) {
                return false;
            }
            if (!substitutionMetadata.columnIdVersions(catalogHandle).containsAll(entry.getValue().columnIdVersions())) {
                return false;
            }
        }
        return true;
    }

    private void collectVersions(Operation node, Map<String, Integer> versionMap, Map<CatalogName, ConnectorIdVersions> catalogVersions)
    {
        versionMap.put(node.name(), node.version());
        switch (node) {
            case Output output -> collectVersions(output.source(), versionMap, catalogVersions);
            case TableScan tableScan -> {
                catalogVersions.compute(tableScan.table().catalogName(), (catalogName, currentVersions) -> {
                    Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
                    if (catalog.isEmpty()) {
                        throw new TrinoException(CATALOG_NOT_FOUND, format("Catalog '%s' not found", catalogName));
                    }

                    ConnectorIdVersion tableIdVersion = tableScan.table().connectorId().version();
                    Optional<ConnectorIdVersion> columnIdVersion = tableScan.assignments().keySet()
                            .stream()
                            .findAny()
                            .map(ConnectorColumnId::version);
                    if (currentVersions == null) {
                        return new ConnectorIdVersions(ImmutableSet.of(tableIdVersion), columnIdVersion.map(ImmutableSet::of).orElse(ImmutableSet.of()));
                    }
                    else {
                        return currentVersions.add(tableIdVersion, columnIdVersion);
                    }
                });
            }
        }
    }
}
