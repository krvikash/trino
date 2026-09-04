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
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.server.substitution.TestUtils.CATALOG;
import static io.starburst.server.substitution.TestUtils.materialization;
import static io.starburst.server.substitution.TestUtils.versionAwareMetastore;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link VersionAwareMaterializationMetastore}: a materialization stored via
 * {@code createOrReplace} (which serializes the IR to JSON) must come back from {@code listMaterializations}
 * (which deserializes it and filters on version compatibility) equal to what went in.
 */
class TestVersionAwareMaterializationMetastore
{
    @Test
    void testRoundTripPreservesDefinition()
    {
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore();
        MaterializationDefinition mv = materialization("mv", "source");

        metastore.createOrReplace(mv);

        assertThat(metastore.listMaterializations()).containsExactly(mv);
    }

    @Test
    void testRoundTripPreservesMultipleDefinitions()
    {
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore();
        MaterializationDefinition first = materialization("mv1", "source");
        MaterializationDefinition second = materialization("mv2", "other_source");

        metastore.createOrReplace(first);
        metastore.createOrReplace(second);

        assertThat(metastore.listMaterializations()).containsExactlyInAnyOrder(first, second);
    }

    @Test
    void testSkipsMaterializationWithIncompatibleConnectorVersion()
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore(raw);
        metastore.createOrReplace(materialization("mv", "source"));

        // stand in for the row having been written by a build whose table identity format was one version older
        raw.createOrReplace(withCatalogIrVersions(
                getOnlyElement(raw.listMaterializations()),
                new ConnectorIdVersions(
                        ImmutableSet.of(new ConnectorIdVersion("TestTableId", 0)),
                        ImmutableSet.of(new ConnectorIdVersion("TestColumnId", 1)))));

        assertThat(metastore.listMaterializations()).isEmpty();
    }

    @Test
    void testSkipsMaterializationThatCannotBeDeserialized()
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore(raw);
        metastore.createOrReplace(materialization("mv", "source"));

        // stand in for the connector that wrote this materialization no longer being installed
        RawMaterializationDefinition stored = getOnlyElement(raw.listMaterializations());
        raw.createOrReplace(new RawMaterializationDefinition(
                stored.irVersions(),
                stored.catalogIrVersions(),
                stored.computationPlanRootJson().replace("SupportedTableId", "LongGoneTableId"),
                stored.storageTableId(),
                stored.source(),
                stored.lastKnownFreshTime(),
                stored.gracePeriod()));

        assertThat(metastore.listMaterializations()).isEmpty();
    }

    private static RawMaterializationDefinition withCatalogIrVersions(RawMaterializationDefinition definition, ConnectorIdVersions versions)
    {
        Map<CatalogName, ConnectorIdVersions> catalogIrVersions = ImmutableMap.of(new CatalogName(CATALOG), versions);
        assertThat(definition.catalogIrVersions()).containsOnlyKeys(catalogIrVersions.keySet());
        return new RawMaterializationDefinition(
                definition.irVersions(),
                catalogIrVersions,
                definition.computationPlanRootJson(),
                definition.storageTableId(),
                definition.source(),
                definition.lastKnownFreshTime(),
                definition.gracePeriod());
    }
}
