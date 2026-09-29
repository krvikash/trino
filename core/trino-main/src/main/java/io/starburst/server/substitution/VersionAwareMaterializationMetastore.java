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
import io.airlift.stats.CounterStat;
import io.starburst.materialization.ir.IrNode;
import io.starburst.materialization.ir.IrVersions;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class VersionAwareMaterializationMetastore
        implements MaterializationMetastore
{
    private static final Logger log = Logger.get(VersionAwareMaterializationMetastore.class);

    private final RawMaterializationMetastore raw;
    private final JsonCodec<Output> irJsonCodec;
    private final CounterStat skippedCount = new CounterStat();

    @Inject
    public VersionAwareMaterializationMetastore(
            RawMaterializationMetastore raw,
            JsonCodec<Output> irJsonCodec)
    {
        this.raw = requireNonNull(raw, "raw is null");
        this.irJsonCodec = requireNonNull(irJsonCodec, "irJsonCodec is null");
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
        String irJson = irJsonCodec.toJson(typed);
        raw.createOrReplace(new RawMaterializationDefinition(
                IrVersions.collect(typed),
                collectConnectorIdVersions(typed),
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

    /**
     * Reads one stored row, or returns empty and counts it as skipped if this engine cannot read it: unsupported
     * IR version, an undeserializable connector identity, or stale connector id versions. A metastore shared by
     * engines of different versions legitimately holds such rows.
     */
    private Optional<MaterializationDefinition> toTyped(RawMaterializationDefinition rawDefinition)
    {
        if (!isIrCompatible(rawDefinition)) {
            log.debug(
                    "Skipping materialization %s — stored version map %s does not match engine supported %s",
                    rawDefinition.source(),
                    rawDefinition.irVersions(),
                    IrVersions.SUPPORTED);
            skippedCount.update(1);
            return Optional.empty();
        }
        // The connector identity classes are resolved through the handle resolver, so a connector that was
        // uninstalled, or whose identity classes changed shape, fails to deserialize rather than yielding a
        // usable plan. Such a materialization is by definition incompatible with the connectors running here.
        Output typed;
        try {
            typed = irJsonCodec.fromJson(rawDefinition.computationPlanRootJson());
        }
        catch (RuntimeException e) {
            // The IR JSON is deliberately not logged: it can be large and carries customer schema details.
            log.debug("Skipping materialization %s — stored computation plan cannot be read by the installed connectors: %s", rawDefinition.source(), e);
            skippedCount.update(1);
            return Optional.empty();
        }
        // A connector identity's version is a constant of the connector code, so re-collecting the versions from
        // the deserialized plan yields what the installed connectors produce today. Stored versions that differ
        // mean the identity format changed since this materialization was written. The IR name versions are
        // deliberately not compared: isIrCompatible accepts a row stamped with any set of names this engine
        // supports, and requiring equality would reject rows written by an engine that stamped a different set.
        Map<CatalogName, ConnectorIdVersions> currentVersions = collectConnectorIdVersions(typed);
        if (!currentVersions.equals(rawDefinition.catalogIrVersions())) {
            log.debug(
                    "Skipping materialization %s — stored connector id versions %s do not match the versions the installed connectors produce %s",
                    rawDefinition.source(),
                    rawDefinition.catalogIrVersions(),
                    currentVersions);
            skippedCount.update(1);
            return Optional.empty();
        }
        return Optional.of(new MaterializationDefinition(
                typed,
                rawDefinition.storageTableId(),
                rawDefinition.source(),
                rawDefinition.lastKnownFreshTime(),
                rawDefinition.gracePeriod()));
    }

    CounterStat getSkippedCount()
    {
        return skippedCount;
    }

    private static boolean isIrCompatible(RawMaterializationDefinition definition)
    {
        for (Map.Entry<String, Integer> e : definition.irVersions().entrySet()) {
            Integer expected = IrVersions.SUPPORTED.get(e.getKey());
            if (expected == null || !expected.equals(e.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static Map<CatalogName, ConnectorIdVersions> collectConnectorIdVersions(Output computationPlanRoot)
    {
        Map<CatalogName, ConnectorIdVersions> catalogVersions = new HashMap<>();
        collectConnectorIdVersions(computationPlanRoot, catalogVersions);
        return ImmutableMap.copyOf(catalogVersions);
    }

    private static void collectConnectorIdVersions(IrNode node, Map<CatalogName, ConnectorIdVersions> catalogVersions)
    {
        switch (node) {
            case TableScan tableScan -> catalogVersions.compute(tableScan.table().catalogName(), (_, currentVersions) -> {
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
            case Output _ -> {}
        }
        for (IrNode child : node.children()) {
            collectConnectorIdVersions(child, catalogVersions);
        }
    }
}
