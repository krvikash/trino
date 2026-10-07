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
import io.starburst.materialization.ir.Aggregation;
import io.starburst.materialization.ir.Aggregation.AggregateFunctionCall;
import io.starburst.materialization.ir.Aggregation.GroupingSets;
import io.starburst.materialization.ir.Aggregation.OrderingScheme;
import io.starburst.materialization.ir.Aggregation.SortItem;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Reference;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.starburst.server.substitution.TestUtils.SupportedTableId;
import io.starburst.server.substitution.TestUtils.TestColumnId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.materialization.ir.Aggregation.Step.PARTIAL;
import static io.starburst.materialization.ir.Aggregation.Step.SINGLE;
import static io.starburst.server.substitution.TestUtils.CATALOG;
import static io.starburst.server.substitution.TestUtils.copyWithIrJson;
import static io.starburst.server.substitution.TestUtils.materialization;
import static io.starburst.server.substitution.TestUtils.simpleAggregate;
import static io.starburst.server.substitution.TestUtils.versionAwareMetastore;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
    void testRoundTripPreservesAggregate()
    {
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore();
        MaterializationDefinition mv = materialization("mv", aggregateWithEveryField());

        metastore.createOrReplace(mv);

        assertThat(metastore.listMaterializations()).containsExactly(mv);
    }

    @Test
    void testStampsVersionsOfAggregate()
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        versionAwareMetastore(raw).createOrReplace(materialization("mv", simpleAggregate(new SupportedTableId("source"), "sum")));

        assertThat(getOnlyElement(raw.listMaterializations()).irVersions()).isEqualTo(ImmutableMap.of(
                "Output", 1,
                "Aggregation", 1,
                "TableScan", 1,
                "fn:system.builtin.sum", 1,
                "expr:Reference", 1,
                "leaf:Symbol", 1,
                "leaf:BoundSignature", 1));
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

    /**
     * A stored row is read iff every name it is stamped with is supported at the same version, whichever set of
     * names the writer stamped.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("storedIrVersions")
    void testIrVersionGate(String scenario, Map<String, Integer> extraIrVersions, boolean readable)
    {
        InMemoryRawMaterializationMetastore raw = new InMemoryRawMaterializationMetastore();
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore(raw);
        MaterializationDefinition mv = materialization("mv", "source");
        metastore.createOrReplace(mv);

        RawMaterializationDefinition stored = getOnlyElement(raw.listMaterializations());
        raw.createOrReplace(withIrVersions(stored, ImmutableMap.<String, Integer>builder()
                .putAll(stored.irVersions())
                .putAll(extraIrVersions)
                .buildOrThrow()));

        if (readable) {
            assertThat(metastore.listMaterializations()).containsExactly(mv);
            assertThat(metastore.getSkippedCount().getTotalCount()).isZero();
        }
        else {
            assertThat(metastore.listMaterializations()).isEmpty();
            assertThat(metastore.getSkippedCount().getTotalCount()).isEqualTo(1);
        }
    }

    private static Stream<Arguments> storedIrVersions()
    {
        return Stream.of(
                Arguments.of("supported expression names", ImmutableMap.of(
                        "expr:Call", 1,
                        "leaf:BoundSignature", 1,
                        "fn:system.builtin.$operator$equal", 1,
                        "expr:Cast", 1,
                        "leaf:Type", 1,
                        "cast:varchar->varchar", 1,
                        "expr:Reference", 1,
                        "leaf:Symbol", 1), true),
                Arguments.of("supported aggregation names", ImmutableMap.of(
                        "Aggregation", 1,
                        "leaf:Symbol", 1,
                        "leaf:BoundSignature", 1,
                        "fn:system.builtin.sum", 1), true),
                Arguments.of("unknown operation", ImmutableMap.of("Filter", 1), false),
                Arguments.of("unknown expression node", ImmutableMap.of("expr:Case", 1), false),
                Arguments.of("unknown function", ImmutableMap.of("fn:system.builtin.random", 1), false),
                Arguments.of("unknown cast pair", ImmutableMap.of("cast:bigint->varchar", 1), false),
                Arguments.of("unknown leaf", ImmutableMap.of("leaf:Constant", 1), false),
                Arguments.of("expression node at another version", ImmutableMap.of("expr:Call", 2), false),
                Arguments.of("function at another version", ImmutableMap.of("fn:system.builtin.$operator$equal", 2), false),
                Arguments.of("aggregate function at another version", ImmutableMap.of("fn:system.builtin.sum", 2), false),
                Arguments.of("cast pair at another version", ImmutableMap.of("cast:varchar->varchar", 2), false),
                Arguments.of("leaf at another version", ImmutableMap.of("leaf:Symbol", 2), false));
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

    /**
     * Aggregates setting every optional field, with a non-{@code SINGLE} step and more than one grouping set. The
     * ordering sits on an outer {@code SINGLE} aggregate, the only step that allows ORDER BY.
     */
    private static Output aggregateWithEveryField()
    {
        Symbol name = new Symbol(VARCHAR, "name");
        Symbol value = new Symbol(BIGINT, "value");
        Symbol flag = new Symbol(BOOLEAN, "flag");
        Symbol values = new Symbol(new ArrayType(BIGINT), "values");
        Symbol sorted = new Symbol(new ArrayType(new ArrayType(BIGINT)), "sorted");
        TableScan scan = new TableScan(
                new TableId(new CatalogName(CATALOG), new SupportedTableId("source")),
                ImmutableMap.of(new TestColumnId("name"), name, new TestColumnId("value"), value, new TestColumnId("flag"), flag));
        AggregateFunctionCall arrayAgg = new AggregateFunctionCall(
                values,
                new BoundSignature(builtinFunctionName("array_agg"), new ArrayType(BIGINT), ImmutableList.of(BIGINT)),
                ImmutableList.of(new Reference(value)),
                true,
                Optional.of(flag),
                Optional.empty(),
                Optional.of(flag));
        Aggregation partial = new Aggregation(scan, new GroupingSets(ImmutableList.of(name, flag), 2, ImmutableSet.of(1)), ImmutableList.of(arrayAgg), PARTIAL, Optional.of(flag));
        AggregateFunctionCall sortedArrayAgg = new AggregateFunctionCall(
                sorted,
                new BoundSignature(builtinFunctionName("array_agg"), new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(new ArrayType(BIGINT))),
                ImmutableList.of(new Reference(values)),
                false,
                Optional.empty(),
                Optional.of(new OrderingScheme(ImmutableList.of(new SortItem(values, DESC_NULLS_LAST)))),
                Optional.empty());
        return new Output(
                ImmutableList.of("name", "sorted"),
                ImmutableList.of(name, sorted),
                new Aggregation(partial, new GroupingSets(ImmutableList.of(name), 1, ImmutableSet.of()), ImmutableList.of(sortedArrayAgg), SINGLE, Optional.empty()));
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
