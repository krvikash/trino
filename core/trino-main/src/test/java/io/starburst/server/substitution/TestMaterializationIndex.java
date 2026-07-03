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
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.starburst.server.substitution.TestUtils.SupportedTableId;
import io.starburst.server.substitution.TestUtils.TestColumnId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.starburst.server.substitution.MaterializationIndex.computeHash;
import static io.starburst.server.substitution.TestUtils.CATALOG;
import static io.starburst.server.substitution.TestUtils.SCHEMA;
import static io.starburst.server.substitution.TestUtils.materialization;
import static io.starburst.server.substitution.TestUtils.mvName;
import static io.starburst.server.substitution.TestUtils.simpleTableScan;
import static io.starburst.server.substitution.TestUtils.versionAwareMetastore;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link MaterializationIndex} keeps its in-memory index in sync with the underlying metastore.
 * Writes made directly on the underlying {@link VersionAwareMaterializationMetastore} (rather than through the
 * index) stand in for changes another cluster makes against a shared metastore; {@link MaterializationIndex#refresh()}
 * must pick them up.
 */
class TestMaterializationIndex
{
    @Test
    void testWriteThroughIsVisibleImmediately()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv = materialization("mv", "source");
        index.createOrReplace(mv);

        assertThat(index.getMaterializations(hash(mv))).containsExactly(mv);
    }

    @Test
    void testRefreshPicksUpExternallyCreatedMaterialization()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        // Another cluster writes directly to the shared metastore, bypassing this node's write-through index.
        MaterializationDefinition mv = materialization("mv", "source");
        underlying.createOrReplace(mv);

        assertThat(index.getMaterializations(hash(mv))).isEmpty();

        index.refresh();

        assertThat(index.getMaterializations(hash(mv))).containsExactly(mv);
    }

    @Test
    void testRefreshDropsExternallyRemovedMaterialization()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv = materialization("mv", "source");
        index.createOrReplace(mv);

        // Another cluster removes it directly; the local index still has it until the next refresh.
        underlying.remove(mvName("mv"));
        assertThat(index.getMaterializations(hash(mv))).containsExactly(mv);

        index.refresh();

        assertThat(index.getMaterializations(hash(mv))).isEmpty();
    }

    @Test
    void testRefreshReplacesExternallyUpdatedMaterialization()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition original = materialization("mv", "source");
        index.createOrReplace(original);

        // Same MV over the same source (same computation hash), but a newer storage snapshot, written externally.
        MaterializationDefinition updated = new MaterializationDefinition(
                original.computationPlanRoot(),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v2")),
                original.source(),
                Instant.now(),
                Optional.empty());
        underlying.createOrReplace(updated);

        index.refresh();

        assertThat(index.getMaterializations(hash(original))).containsExactly(updated);
    }

    @Test
    void testRefreshReflectsExternalRename()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv = materialization("mv_old", "source");
        index.createOrReplace(mv);

        underlying.renameIfExists(
                mvName("mv_old"),
                mvName("mv_new"),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v1")));

        index.refresh();

        // Rename keeps the computation (and thus the hash); only the indexed MV name changes.
        List<MaterializationDefinition> indexed = index.getMaterializations(hash(mv));
        assertThat(indexed).hasSize(1);
        assertThat(((MaterializedViewSource) indexed.getFirst().source()).materializedViewName()).isEqualTo(mvName("mv_new"));
    }

    @Test
    void testMaterializationsAreGroupedByComputationHash()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition overSource1 = materialization("mv1", "source");
        MaterializationDefinition alsoOverSource1 = materialization("mv2", "source");
        MaterializationDefinition overSource2 = materialization("mv3", "other_source");
        underlying.createOrReplace(overSource1);
        underlying.createOrReplace(alsoOverSource1);
        underlying.createOrReplace(overSource2);

        index.refresh();

        assertThat(index.getMaterializations(hash(overSource1))).containsExactlyInAnyOrder(overSource1, alsoOverSource1);
        assertThat(index.getMaterializations(hash(overSource2))).containsExactly(overSource2);
    }

    @Test
    void testWriteThroughReplacesSameMaterialization()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition original = materialization("mv", "source");
        index.createOrReplace(original);

        // Same MV and computation (same hash), newer storage snapshot — replaces the entry in place.
        MaterializationDefinition replacement = new MaterializationDefinition(
                original.computationPlanRoot(),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v2")),
                original.source(),
                Instant.now(),
                Optional.empty());
        index.createOrReplace(replacement);

        assertThat(index.getMaterializations(hash(original))).containsExactly(replacement);
    }

    @Test
    void testWriteThroughGroupsMaterializationsWithSameHash()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition first = materialization("mv1", "source");
        MaterializationDefinition second = materialization("mv2", "source");
        index.createOrReplace(first);
        index.createOrReplace(second);

        assertThat(index.getMaterializations(hash(first))).containsExactlyInAnyOrder(first, second);
    }

    @Test
    void testWriteThroughMovesMaterializationWhenComputationChanges()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition before = materialization("mv", "source");
        MaterializationDefinition after = materialization("mv", "other_source");
        index.createOrReplace(before);
        index.createOrReplace(after);

        // The MV moved to a different computation hash, so its old bucket no longer offers it.
        assertThat(index.getMaterializations(hash(before))).isEmpty();
        assertThat(index.getMaterializations(hash(after))).containsExactly(after);
    }

    @Test
    void testWriteThroughRemove()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv = materialization("mv", "source");
        index.createOrReplace(mv);
        index.remove(mvName("mv"));

        assertThat(index.getMaterializations(hash(mv))).isEmpty();
    }

    @Test
    void testRemoveKeepsOtherMaterializationsInSameBucket()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition first = materialization("mv1", "source");
        MaterializationDefinition second = materialization("mv2", "source");
        index.createOrReplace(first);
        index.createOrReplace(second);

        index.remove(mvName("mv1"));

        assertThat(index.getMaterializations(hash(first))).containsExactly(second);
    }

    @Test
    void testRemoveUnknownMaterializationIsNoOp()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        index.remove(mvName("absent"));

        assertThat(index.getMaterializations(hash(materialization("absent", "source")))).isEmpty();
    }

    @Test
    void testWriteThroughRename()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv = materialization("mv_old", "source");
        index.createOrReplace(mv);

        index.renameIfExists(
                mvName("mv_old"),
                mvName("mv_new"),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v1")));

        List<MaterializationDefinition> indexed = index.getMaterializations(hash(mv));
        assertThat(indexed).hasSize(1);
        assertThat(((MaterializedViewSource) indexed.getFirst().source()).materializedViewName()).isEqualTo(mvName("mv_new"));
    }

    @Test
    void testRenameKeepsOtherMaterializationsInSameBucket()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        MaterializationDefinition first = materialization("mv1", "source");
        MaterializationDefinition second = materialization("mv2", "source");
        index.createOrReplace(first);
        index.createOrReplace(second);

        index.renameIfExists(
                mvName("mv1"),
                mvName("mv1_renamed"),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v1")));

        List<MaterializationDefinition> indexed = index.getMaterializations(hash(first));
        assertThat(indexed.stream().map(definition -> ((MaterializedViewSource) definition.source()).materializedViewName()))
                .containsExactlyInAnyOrder(mvName("mv2"), mvName("mv1_renamed"));
    }

    @Test
    void testRenameUnknownMaterializationIsNoOp()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        index.renameIfExists(
                mvName("absent"),
                mvName("renamed"),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v1")));

        assertThat(index.getMaterializations(hash(materialization("renamed", "source")))).isEmpty();
    }

    @Test
    void testRefreshReplaysConcurrentWriteThrough()
    {
        AtomicReference<Runnable> duringList = new AtomicReference<>(() -> {});
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore(() -> duringList.get().run());
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition external = materialization("external", "source");
        underlying.createOrReplace(external);

        // A write-through that lands after the refresh has taken its listing snapshot: it is absent from the
        // base the refresh rebuilds, so it survives only because the refresh replays the buffered delta.
        MaterializationDefinition concurrent = materialization("concurrent", "other_source");
        duringList.set(() -> {
            duringList.set(() -> {});
            index.createOrReplace(concurrent);
        });

        index.refresh();

        assertThat(index.getMaterializations(hash(external))).containsExactly(external);
        assertThat(index.getMaterializations(hash(concurrent))).containsExactly(concurrent);
    }

    @Test
    void testRefreshStatsTrackListedAndRefreshedCounts()
    {
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore();
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        MaterializationDefinition mv1 = materialization("mv1", "source");
        MaterializationDefinition mv2 = materialization("mv2", "other_source");
        underlying.createOrReplace(mv1);
        underlying.createOrReplace(mv2);

        index.refresh();
        assertThat(index.getRefreshStats().getListedCount().getTotalCount()).isEqualTo(2);
        assertThat(index.getRefreshStats().getRefreshedCount().getTotalCount()).isEqualTo(2);

        // A refresh that changes nothing lists the same rows but refreshes none of them.
        index.refresh();
        assertThat(index.getRefreshStats().getListedCount().getTotalCount()).isEqualTo(4);
        assertThat(index.getRefreshStats().getRefreshedCount().getTotalCount()).isEqualTo(2);

        // One added, one removed, one modified: three entries change while only two remain listed.
        underlying.createOrReplace(materialization("mv3", "third_source"));
        underlying.remove(mvName("mv2"));
        underlying.createOrReplace(new MaterializationDefinition(
                mv1.computationPlanRoot(),
                new StorageTableId(new CatalogName(CATALOG), new ConnectorStorageTableId(SCHEMA, "mv_storage", "v2")),
                mv1.source(),
                Instant.now(),
                Optional.empty()));

        index.refresh();
        assertThat(index.getRefreshStats().getListedCount().getTotalCount()).isEqualTo(6);
        assertThat(index.getRefreshStats().getRefreshedCount().getTotalCount()).isEqualTo(5);
    }

    @Test
    void testRefreshFailureIsCountedAndPropagated()
    {
        AtomicReference<Runnable> duringList = new AtomicReference<>(() -> {});
        VersionAwareMaterializationMetastore underlying = versionAwareMetastore(() -> duringList.get().run());
        MaterializationIndex index = new MaterializationIndex(underlying, new MaterializedViewSubstitutionConfig());

        duringList.set(() -> {
            throw new RuntimeException("metastore unavailable");
        });

        assertThatThrownBy(index::refresh)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("metastore unavailable");
        assertThat(index.getRefreshStats().getFailureCount().getTotalCount()).isEqualTo(1);

        // A subsequent successful refresh does not bump the failure count.
        duringList.set(() -> {});
        index.refresh();
        assertThat(index.getRefreshStats().getFailureCount().getTotalCount()).isEqualTo(1);
    }

    @Test
    void testComputeHashRejectsNonTableScanRoot()
    {
        Output nonScanRoot = new Output(
                ImmutableList.of("name"),
                ImmutableList.of(new Symbol(VARCHAR, "name")),
                simpleTableScan(new SupportedTableId("source"), new TestColumnId("name")));

        assertThatThrownBy(() -> computeHash(nonScanRoot))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testStartIsIdempotentAndStops()
    {
        MaterializationIndex index = new MaterializationIndex(versionAwareMetastore(), new MaterializedViewSubstitutionConfig());

        index.start();
        index.start(); // already started — second call is a no-op
        index.stop();

        assertThat(index.getMaterializations(hash(materialization("x", "source")))).isEmpty();
    }

    private static ComputationHash hash(MaterializationDefinition materialization)
    {
        return computeHash(materialization.computationPlanRoot());
    }
}
