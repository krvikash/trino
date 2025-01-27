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

import io.starburst.schema.discovery.TableChanges.TableName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscoveredIdentifier
{
    @Test
    public void testDiscoveredIdentifiers()
    {
        HiveIdentifier validHiveTable = new HiveIdentifier("valid_table");
        TrinoIdentifier validTrinoTable = new TrinoIdentifier("valid_table");

        {
            assertThat(validHiveTable).isEqualTo(validTrinoTable);
            assertThat(validHiveTable).hasSameHashCodeAs(validTrinoTable);
            assertThat(new TableName(Optional.empty(), validHiveTable)).isEqualTo(new TableName(Optional.empty(), validTrinoTable));
            assertThat(new TableName(Optional.empty(), validHiveTable)).hasSameHashCodeAs(new TableName(Optional.empty(), validTrinoTable));

            assertThat(validTrinoTable).isEqualTo(validHiveTable);
            assertThat(validTrinoTable).hasSameHashCodeAs(validHiveTable);
            assertThat(new TableName(Optional.empty(), validTrinoTable)).isEqualTo(new TableName(Optional.empty(), validHiveTable));
            assertThat(new TableName(Optional.empty(), validTrinoTable)).hasSameHashCodeAs(new TableName(Optional.empty(), validHiveTable));
        }

        HiveIdentifier convertedHiveTable = HiveIdentifier.toHiveIdentifier("not=valid=hive=should=convert_table");
        TrinoIdentifier notConvertedTrinoTable = new TrinoIdentifier("not=valid=hive=should=convert_table");

        {
            assertThat(convertedHiveTable).isNotEqualTo(notConvertedTrinoTable);
            assertThat(convertedHiveTable).doesNotHaveSameHashCodeAs(notConvertedTrinoTable);
            assertThat(new TableName(Optional.empty(), convertedHiveTable)).isNotEqualTo(new TableName(Optional.empty(), notConvertedTrinoTable));
            assertThat(new TableName(Optional.empty(), convertedHiveTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.empty(), notConvertedTrinoTable));
            assertThat(notConvertedTrinoTable).isNotEqualTo(convertedHiveTable);
            assertThat(notConvertedTrinoTable).doesNotHaveSameHashCodeAs(convertedHiveTable);
            assertThat(new TableName(Optional.empty(), notConvertedTrinoTable)).isNotEqualTo(new TableName(Optional.empty(), convertedHiveTable));
            assertThat(new TableName(Optional.empty(), notConvertedTrinoTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.empty(), convertedHiveTable));
        }

        HiveIdentifier validHiveSchema = new HiveIdentifier("valid_schema");
        TrinoIdentifier validTrinoSchema = new TrinoIdentifier("valid_schema");

        {
            assertThat(validHiveSchema).isEqualTo(validTrinoSchema);
            assertThat(validHiveSchema).hasSameHashCodeAs(validTrinoSchema);
            assertThat(new TableName(Optional.of(validHiveSchema), validHiveTable)).isEqualTo(new TableName(Optional.of(validTrinoSchema), validTrinoTable));
            assertThat(new TableName(Optional.of(validHiveSchema), validHiveTable)).hasSameHashCodeAs(new TableName(Optional.of(validTrinoSchema), validTrinoTable));

            assertThat(validTrinoSchema).isEqualTo(validHiveSchema);
            assertThat(validTrinoSchema).hasSameHashCodeAs(validHiveSchema);
            assertThat(new TableName(Optional.of(validTrinoSchema), validTrinoTable)).isEqualTo(new TableName(Optional.of(validHiveSchema), validHiveTable));
            assertThat(new TableName(Optional.of(validTrinoSchema), validTrinoTable)).hasSameHashCodeAs(new TableName(Optional.of(validHiveSchema), validHiveTable));
        }

        HiveIdentifier convertedHiveSchema = HiveIdentifier.toHiveIdentifier("not=valid=hive=should=convert_schema");
        TrinoIdentifier notConvertedTrinoSchema = new TrinoIdentifier("not=valid=hive=should=convert_schema");

        {
            assertThat(convertedHiveSchema).isNotEqualTo(notConvertedTrinoSchema);
            assertThat(convertedHiveSchema).doesNotHaveSameHashCodeAs(notConvertedTrinoSchema);
            assertThat(new TableName(Optional.of(convertedHiveSchema), validHiveTable)).isNotEqualTo(new TableName(Optional.of(notConvertedTrinoSchema), validHiveTable));
            assertThat(new TableName(Optional.of(convertedHiveSchema), validHiveTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.of(notConvertedTrinoSchema), validHiveTable));
            assertThat(notConvertedTrinoSchema).isNotEqualTo(convertedHiveSchema);
            assertThat(notConvertedTrinoSchema).doesNotHaveSameHashCodeAs(convertedHiveSchema);
            assertThat(new TableName(Optional.of(notConvertedTrinoSchema), validHiveTable)).isNotEqualTo(new TableName(Optional.of(convertedHiveSchema), validHiveTable));
            assertThat(new TableName(Optional.of(notConvertedTrinoSchema), validHiveTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.of(convertedHiveSchema), validHiveTable));
        }

        // both table & schema not the same
        {
            assertThat(new TableName(Optional.of(convertedHiveSchema), validHiveTable)).isNotEqualTo(new TableName(Optional.of(notConvertedTrinoSchema), notConvertedTrinoTable));
            assertThat(new TableName(Optional.of(convertedHiveSchema), validHiveTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.of(notConvertedTrinoSchema), notConvertedTrinoTable));

            assertThat(new TableName(Optional.of(notConvertedTrinoSchema), notConvertedTrinoTable)).isNotEqualTo(new TableName(Optional.of(convertedHiveSchema), validHiveTable));
            assertThat(new TableName(Optional.of(notConvertedTrinoSchema), notConvertedTrinoTable)).doesNotHaveSameHashCodeAs(new TableName(Optional.of(convertedHiveSchema), validHiveTable));
        }
    }
}
