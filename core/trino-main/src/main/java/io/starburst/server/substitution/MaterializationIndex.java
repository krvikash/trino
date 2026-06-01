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
import com.google.inject.Inject;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MaterializationIndex
        implements MaterializationMetastore
{
    private final VersionAwareMaterializationMetastore materializationMetastore;
    private final Map<ComputationHash, List<MaterializationDefinition>> indexedMaterializations = new ConcurrentHashMap<>();
    private final Map<CatalogSchemaTableName, ComputationHash> materializationHash = new ConcurrentHashMap<>();

    @Inject
    public MaterializationIndex(VersionAwareMaterializationMetastore materializationMetastore)
    {
        this.materializationMetastore = requireNonNull(materializationMetastore, "materializationMetastore is null");
    }

    public List<MaterializationDefinition> getMaterializations(ComputationHash hash)
    {
        return indexedMaterializations.getOrDefault(hash, ImmutableList.of());
    }

    static ComputationHash computeHash(Operation root)
    {
        if (root instanceof Output output) {
            root = output.source();
        }
        if (root instanceof TableScan tableScan) {
            return new ComputationHash(tableScan.table().hash());
        }
        throw new UnsupportedOperationException("Unsupported operation: " + root.getClass().getName());
    }

    @Override
    public List<MaterializationDefinition> listMaterializations()
    {
        return materializationMetastore.listMaterializations();
    }

    @Override
    public void createOrReplace(MaterializationDefinition materialization)
    {
        materializationMetastore.createOrReplace(materialization);
        materializationHash.compute(materializedViewName(materialization), (_, oldHash) -> {
            ComputationHash hash = computeHash(materialization.computationPlanRoot());
            indexedMaterializations.compute(hash,
                    (_, materializations) -> {
                        if (materializations == null) {
                            return ImmutableList.of(materialization);
                        }
                        ImmutableList.Builder<MaterializationDefinition> builder = ImmutableList.builder();
                        boolean replaced = false;
                        for (MaterializationDefinition indexedMaterialization : materializations) {
                            if (indexedMaterialization.source().equals(materialization.source())) {
                                builder.add(materialization);
                                replaced = true;
                            }
                            else {
                                builder.add(indexedMaterialization);
                            }
                        }
                        if (!replaced) {
                            builder.add(materialization);
                        }
                        return builder.build();
                    });
            if (oldHash != null && !oldHash.equals(hash)) {
                indexedMaterializations.compute(oldHash,
                        (_, materializations) -> {
                            if (materializations == null) {
                                return null;
                            }
                            List<MaterializationDefinition> filtered = materializations.stream()
                                    .filter(oldHashMaterialization -> !oldHashMaterialization.source().equals(materialization.source()))
                                    .collect(toImmutableList());
                            return filtered.isEmpty() ? null : filtered;
                        });
            }
            return hash;
        });
    }

    private CatalogSchemaTableName materializedViewName(MaterializationDefinition materialization)
    {
        checkArgument(materialization.source() instanceof MaterializedViewSource, "Materialization source is not a materialized view: %s", materialization.source());
        return ((MaterializedViewSource) materialization.source()).materializedViewName();
    }

    @Override
    public void remove(CatalogSchemaTableName materializedViewName)
    {
        materializationMetastore.remove(materializedViewName);
        materializationHash.compute(materializedViewName, (_, currentHash) -> {
            if (currentHash != null) {
                indexedMaterializations.compute(currentHash,
                        (_, materializations) -> {
                            if (materializations == null) {
                                return null;
                            }
                            List<MaterializationDefinition> filtered = materializations.stream()
                                    .filter(oldHashMaterialization -> !materializedViewName(oldHashMaterialization).equals(materializedViewName))
                                    .collect(toImmutableList());
                            return filtered.isEmpty() ? null : filtered;
                        });
            }
            return null;
        });
    }

    @Override
    public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        materializationMetastore.renameIfExists(source, target, targetStorageTableId);
        ComputationHash hash = materializationHash.remove(source);
        if (hash == null) {
            return;
        }
        materializationHash.put(target, hash);
        indexedMaterializations.computeIfPresent(hash, (_, materializations) -> {
            ImmutableList.Builder<MaterializationDefinition> result = ImmutableList.builder();
            for (MaterializationDefinition oldHashMaterialization : materializations) {
                if (materializedViewName(oldHashMaterialization).equals(source)) {
                    result.add(renameTo(oldHashMaterialization, target, targetStorageTableId));
                }
                else {
                    result.add(oldHashMaterialization);
                }
            }
            return result.build();
        });
    }

    private static MaterializationDefinition renameTo(MaterializationDefinition current, CatalogSchemaTableName newName, StorageTableId newStorageTableId)
    {
        return new MaterializationDefinition(
                current.computationPlanRoot(),
                newStorageTableId,
                new MaterializedViewSource(newName),
                current.lastKnownFreshTime(),
                current.gracePeriod());
    }
}
