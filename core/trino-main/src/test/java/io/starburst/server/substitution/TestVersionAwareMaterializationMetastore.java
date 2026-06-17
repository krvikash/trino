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

import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.server.substitution.TestUtils.TestColumnId;
import io.starburst.server.substitution.TestUtils.UnsupportedTableId;
import org.junit.jupiter.api.Test;

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
        VersionAwareMaterializationMetastore metastore = versionAwareMetastore();
        metastore.createOrReplace(materialization("mv", new UnsupportedTableId("source"), new TestColumnId("name")));

        assertThat(metastore.listMaterializations()).isEmpty();
    }
}
