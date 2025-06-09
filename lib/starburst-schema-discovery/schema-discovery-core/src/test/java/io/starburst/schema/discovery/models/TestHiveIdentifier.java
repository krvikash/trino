/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.schema.discovery.models.HiveIdentifier.toHiveIdentifier;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestHiveIdentifier
{
    @Test
    public void testHiveCompatibleNames()
    {
        List<String> validNames = ImmutableList.of(
                "table_name",
                "-table_name",
                "$table",
                "$8table",
                "-$table",
                "$$");
        validNames.forEach(validName ->
                assertThatNoException().isThrownBy(() -> new HiveIdentifier(toLowerCase(validName))));

        List<String> invalidNames = ImmutableList.of(
                "12invalid",
                "table$",
                "$",
                "");
        invalidNames.forEach(invalidName ->
                assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new HiveIdentifier(toLowerCase(invalidName))));
    }

    @Test
    public void testHiveNameCleanup()
    {
        assertThat(toHiveIdentifier(toLowerCase("12invalid"))).isEqualTo(new HiveIdentifier(toLowerCase("invalid")));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new HiveIdentifier(toLowerCase("table$")));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new HiveIdentifier(toLowerCase("$")));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new HiveIdentifier(toLowerCase("")));
    }

    @Test
    public void testHiveIdentifierErrorMessage()
    {
        assertThatException()
                .isThrownBy(() -> toHiveIdentifier("partition_reception_time=15-00-00"))
                .withMessage("Identifier: partition_reception_time=15-00-00 is not valid for Hive compatibility");
    }
}
