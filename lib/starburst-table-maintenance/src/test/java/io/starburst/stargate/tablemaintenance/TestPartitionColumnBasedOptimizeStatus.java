/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.starburst.stargate.tablemaintenance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class TestPartitionColumnBasedOptimizeStatus
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testDeserializeOldFormatSingleColumn()
            throws Exception
    {
        String oldFormatJson =
                """
                {
                    "type": "partitionColumnBased",
                    "tableColumn": "event_date",
                    "partitionColumn": "event_date",
                    "optimizedInQueryPartitionValues": ["CAST('2025-01-01' AS date)", "CAST('2025-01-02' AS date)"]
                }
                """;

        TableOptimizeStatus status = OBJECT_MAPPER.readValue(oldFormatJson, TableOptimizeStatus.class);
        assertThat(status).isInstanceOf(PartitionColumnBasedOptimizeStatus.class);
        PartitionColumnBasedOptimizeStatus partitionStatus = (PartitionColumnBasedOptimizeStatus) status;
        assertThat(partitionStatus.partitionColumns()).containsExactly("event_date");
        assertThat(partitionStatus.optimizedInQueryPartitionValues()).containsExactly(
                "CAST('2025-01-01' AS date)", "CAST('2025-01-02' AS date)");
    }

    @Test
    public void testDeserializeNewFormatMultipleColumns()
            throws Exception
    {
        String newFormatJson =
                """
                {
                    "type": "partitionColumnBased",
                    "partitionColumns": ["col1", "col2"],
                    "tableColumns": ["col1", "col2"],
                    "optimizedInQueryPartitionValues": []
                }
                """;

        TableOptimizeStatus status = OBJECT_MAPPER.readValue(newFormatJson, TableOptimizeStatus.class);
        assertThat(status).isInstanceOf(PartitionColumnBasedOptimizeStatus.class);
        PartitionColumnBasedOptimizeStatus partitionStatus = (PartitionColumnBasedOptimizeStatus) status;
        assertThat(partitionStatus.partitionColumns()).containsExactly("col1", "col2");
        assertThat(partitionStatus.optimizedInQueryPartitionValues()).isEmpty();
    }

    @Test
    public void testRoundTripSerialization()
            throws Exception
    {
        PartitionColumnBasedOptimizeStatus original = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("col1", "col2"), ImmutableList.of("value1"));

        String json = OBJECT_MAPPER.writeValueAsString(original);
        assertThat(json).contains("\"partitionColumns\"");

        TableOptimizeStatus deserialized = OBJECT_MAPPER.readValue(json, TableOptimizeStatus.class);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testWithLatestOptimizeQuerySingleColumn()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_date"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_date" = CAST('2025-01-01' AS date)
                """);

        assertThat(updated).isInstanceOf(PartitionColumnBasedOptimizeStatus.class);
        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('2025-01-01' AS date)");
    }

    @Test
    public void testWithLatestOptimizeQueryMultipleColumnsStripsAllPrefixes()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_date", "event_type"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_date" = CAST('2025-01-01' AS date) AND "event_type" = CAST('CREATE' AS varchar)
                """);

        assertThat(updated).isInstanceOf(PartitionColumnBasedOptimizeStatus.class);
        PartitionColumnBasedOptimizeStatus updatedStatus = (PartitionColumnBasedOptimizeStatus) updated;
        assertThat(updatedStatus.optimizedInQueryPartitionValues())
                .containsExactly("CAST('2025-01-01' AS date) AND CAST('CREATE' AS varchar)");
    }

    @Test
    public void testWithLatestOptimizeQueryAccumulatesAcrossMultipleQueries()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_date", "event_type"), ImmutableList.of());

        TableOptimizeStatus after1 = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_date" = CAST('2025-01-01' AS date) AND "event_type" = CAST('CREATE' AS varchar)
                """);
        TableOptimizeStatus after2 = after1.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_date" = CAST('2025-01-02' AS date) AND "event_type" = CAST('UPDATE' AS varchar)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) after2).optimizedInQueryPartitionValues())
                .containsExactly(
                        "CAST('2025-01-01' AS date) AND CAST('CREATE' AS varchar)",
                        "CAST('2025-01-02' AS date) AND CAST('UPDATE' AS varchar)");
    }

    @Test
    public void testWithLatestOptimizeQueryStripsDayTransformPrefix()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_timestamp"), ImmutableList.of("event_timestamp_day"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE date("event_timestamp") = CAST('2025-02-11' AS date)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('2025-02-11' AS date)");
    }

    @Test
    public void testWithLatestOptimizeQueryStripsMixedIdentityAndDayPrefixes()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_type", "event_timestamp"), ImmutableList.of("event_type", "event_timestamp_day"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_type" = CAST('CREATE' AS varchar) AND date("event_timestamp") = CAST('2025-02-11' AS date)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('CREATE' AS varchar) AND CAST('2025-02-11' AS date)");
    }

    @Test
    public void testWithLatestOptimizeQueryColumnLiterallyEndingInDaySuffixIdentityPartitioning()
    {
        // identity-partitioned column literally named "event_day" — partition spec field name is also "event_day"
        // and the WHERE clause uses the identity form, so we must NOT mis-interpret it as day("event").
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_day"), ImmutableList.of("event_day"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "event_day" = CAST('CREATE' AS varchar)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('CREATE' AS varchar)");
    }

    @Test
    public void testWithLatestOptimizeQueryDayTransformOnColumnLiterallyEndingInDaySuffix()
    {
        // day(event_day) -> partition spec field name is "event_day_day", WHERE clause uses date("event_day").
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_day"), ImmutableList.of("event_day_day"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE date("event_day") = CAST('2025-02-11' AS date)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('2025-02-11' AS date)");
    }

    @Test
    public void testInvalidColumnTransform()
    {
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("event_type"), ImmutableList.of("event_type_illegal"), ImmutableList.of());

        assertThatException()
                .as("Illegal transform should throw")
                .isThrownBy(() -> status.withLatestOptimizeQuery(
                        """
                        ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                        WHERE date("event_day") = CAST('2025-02-11' AS date)
                        """))
                .withMessage("No supported transform produces partition column 'event_type_illegal' from source column 'event_type'");
    }

    @Test
    public void testDeserializeOldFormatWithoutPartitionColumnsField()
            throws Exception
    {
        String oldDbJson =
                """
                {
                    "type": "partitionColumnBased",
                    "tableColumn": "col1",
                    "partitionColumn": "col1",
                    "optimizedInQueryPartitionValues": []
                }
                """;

        PartitionColumnBasedOptimizeStatus status = (PartitionColumnBasedOptimizeStatus)
                OBJECT_MAPPER.readValue(oldDbJson, TableOptimizeStatus.class);
        assertThat(status.tableColumns()).containsExactly("col1");
        assertThat(status.partitionColumns()).containsExactly("col1");
        assertThat(status).isEqualTo(new PartitionColumnBasedOptimizeStatus(ImmutableList.of("col1"), ImmutableList.of()));
    }

    @Test
    public void testConstructorRejectsMismatchedTableAndPartitionColumnSizes()
    {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PartitionColumnBasedOptimizeStatus(
                        ImmutableList.of("event_ts"),
                        ImmutableList.of("event_ts_day", "event_type"),
                        ImmutableList.of()))
                .withMessageContaining("tableColumns and partitionColumns must be same size");
    }

    @Test
    public void testWithLatestOptimizeQueryValueLiteralContainsColumnExpression()
    {
        // Identity-partitioned varchar column whose value happens to contain the column expression and ' = ' anchor.
        // The first match of `"comment" = ` is the actual predicate (at start of stripped WHERE), so the embedded
        // occurrence inside the literal is preserved verbatim.
        PartitionColumnBasedOptimizeStatus status = new PartitionColumnBasedOptimizeStatus(
                ImmutableList.of("comment"), ImmutableList.of("comment"), ImmutableList.of());

        TableOptimizeStatus updated = status.withLatestOptimizeQuery(
                """
                ALTER TABLE "cat"."schema"."tbl" EXECUTE OPTIMIZE (file_size_threshold => '67MB')
                WHERE "comment" = CAST('"comment" = is a delimiter' AS varchar)
                """);

        assertThat(((PartitionColumnBasedOptimizeStatus) updated).optimizedInQueryPartitionValues())
                .containsExactly("CAST('\"comment\" = is a delimiter' AS varchar)");
    }
}
