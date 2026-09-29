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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.server.substitution.TestUtils.CATALOG;
import static io.starburst.server.substitution.TestUtils.copyWithIrJson;
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
    void testStampsVersionsOfPlainScan()
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        versionAwareMetastore(raw).createOrReplace(materialization("mv", "source"));

        RawMaterializationDefinition stored = getOnlyElement(raw.listMaterializations());
        assertThat(stored.irVersions()).isEqualTo(ImmutableMap.of("Output", 1, "TableScan", 1));
        assertThat(stored.catalogIrVersions()).isEqualTo(ImmutableMap.of(
                new CatalogName(CATALOG), new ConnectorIdVersions(
                        ImmutableSet.of(new ConnectorIdVersion("TestTableId", 1)),
                        ImmutableSet.of(new ConnectorIdVersion("TestColumnId", 1)))));
    }

    @Test
    void testSkipsMaterializationWithIncompatibleIrVersion()
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore(raw);
        metastore.createOrReplace(materialization("mv", "source"));

        // stand in for the row having been written by a build whose IR node version this engine does not support
        RawMaterializationDefinition stored = getOnlyElement(raw.listMaterializations());
        raw.createOrReplace(withIrVersions(stored, ImmutableMap.of("TableScan", 99)));

        assertThat(metastore.listMaterializations()).isEmpty();
        assertThat(metastore.getSkippedCount().getTotalCount()).isEqualTo(1);
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
        assertThat(metastore.getSkippedCount().getTotalCount()).isEqualTo(1);
    }

    /**
     * Parameterized: all three failure modes below hit {@code toTyped}'s {@code catch (RuntimeException e)} and
     * are expected to skip and count identically, so one test body covers them.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("deserializationFailures")
    void testSkipsMaterializationThatCannotBeDeserialized(String scenario, UnaryOperator<String> corruptIrJson)
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore(raw);
        MaterializationDefinition good = materialization("mv", "source");
        metastore.createOrReplace(good);

        RawMaterializationDefinition stored = getOnlyElement(raw.listMaterializations());
        raw.createOrReplace(copyWithIrJson(stored, "unreadable", corruptIrJson.apply(stored.computationPlanRootJson())));

        assertThat(metastore.listMaterializations()).containsExactly(good);
        assertThat(metastore.getSkippedCount().getTotalCount()).isEqualTo(1);
    }

    private static Stream<Arguments> deserializationFailures()
    {
        return Stream.of(
                Arguments.of("malformed JSON", malformedJson()),
                Arguments.of("unresolvable connector identity", replacingToken("SupportedTableId", "LongGoneTableId")),
                Arguments.of("unresolvable type", replacingToken("varchar", "no_such_type")));
    }

    private static UnaryOperator<String> malformedJson()
    {
        return _ -> "{ this is not json";
    }

    /**
     * Replaces every {@code token} in the IR JSON with {@code replacement}, asserting {@code token} is present
     * first so a fixture change can't silently turn this into a no-op.
     */
    private static UnaryOperator<String> replacingToken(String token, String replacement)
    {
        return json -> {
            assertThat(json).contains(token);
            return json.replace(token, replacement);
        };
    }

    private static RawMaterializationDefinition withIrVersions(RawMaterializationDefinition definition, Map<String, Integer> irVersions)
    {
        return new RawMaterializationDefinition(
                irVersions,
                definition.catalogIrVersions(),
                definition.computationPlanRootJson(),
                definition.storageTableId(),
                definition.source(),
                definition.lastKnownFreshTime(),
                definition.gracePeriod());
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
