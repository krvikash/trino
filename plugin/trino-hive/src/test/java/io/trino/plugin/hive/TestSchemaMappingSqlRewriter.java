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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.SchemaMappingPrefixes.SchemaPrefix;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.SchemaMappingSqlRewriter.prefixSchemaNames;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSchemaMappingSqlRewriter
{
    private static final SchemaPrefix PREFIX = new SchemaPrefix("egdp_prod_", new SchemaMappingPrefixes(ImmutableSet.of("egdp_prod_", "egdp_test_")));

    @Test
    void testSchemaNameGetsPrefix()
    {
        assertThat(rewrite("SELECT * FROM \"marketing\".\"bookings\""))
                .isEqualTo("SELECT * FROM \"egdp_prod_marketing\".\"bookings\"");
        assertThat(rewrite("SELECT * FROM marketing.bookings"))
                .isEqualTo("SELECT * FROM \"egdp_prod_marketing\".bookings");
    }

    @Test
    void testPrefixedSchemaNameIsUnchanged()
    {
        assertThat(rewrite("SELECT * FROM egdp_prod_marketing.bookings"))
                .isEqualTo("SELECT * FROM egdp_prod_marketing.bookings");
    }

    @Test
    void testOtherPrefixIsUnchanged()
    {
        assertThat(rewrite("SELECT * FROM egdp_test_marketing.bookings"))
                .isEqualTo("SELECT * FROM egdp_test_marketing.bookings");
    }

    @Test
    void testUnqualifiedNamesAreUnchanged()
    {
        assertThat(rewrite("WITH bookings AS (SELECT 1 AS a) SELECT * FROM bookings"))
                .isEqualTo("WITH bookings AS (SELECT 1 AS a) SELECT * FROM bookings");
    }

    @Test
    void testCatalogQualifiedNames()
    {
        assertThat(rewrite("SELECT * FROM hive.marketing.bookings"))
                .isEqualTo("SELECT * FROM hive.\"egdp_prod_marketing\".bookings");
        assertThat(rewrite("SELECT * FROM other.marketing.bookings"))
                .isEqualTo("SELECT * FROM other.marketing.bookings");
    }

    @Test
    void testIdentifierCase()
    {
        assertThat(rewrite("SELECT * FROM Marketing.Bookings"))
                .isEqualTo("SELECT * FROM \"egdp_prod_marketing\".Bookings");
        assertThat(rewrite("SELECT * FROM \"Marketing\".bookings"))
                .isEqualTo("SELECT * FROM \"egdp_prod_Marketing\".bookings");
    }

    @Test
    void testReferencesInSubqueriesAndJoins()
    {
        assertThat(rewrite(
                """
                SELECT *
                FROM supply.lodging l
                JOIN (SELECT id FROM common.geo) g ON g.id = l.id
                WHERE l.name = 'supply.lodging'"""))
                .isEqualTo(
                        """
                        SELECT *
                        FROM "egdp_prod_supply".lodging l
                        JOIN (SELECT id FROM "egdp_prod_common".geo) g ON g.id = l.id
                        WHERE l.name = 'supply.lodging'""");
    }

    @Test
    void testQuoteInDelimitedName()
    {
        assertThat(rewrite("SELECT * FROM \"we\"\"ird\".bookings"))
                .isEqualTo("SELECT * FROM \"egdp_prod_we\"\"ird\".bookings");
    }

    @Test
    void testQualifiedColumnReferenceGetsPrefix()
    {
        assertThat(rewrite("SELECT supply.lodging.id FROM supply.lodging"))
                .isEqualTo("SELECT \"egdp_prod_supply\".lodging.id FROM \"egdp_prod_supply\".lodging");
        assertThat(rewrite("SELECT marketing.bookings.id FROM marketing.bookings"))
                .isEqualTo("SELECT \"egdp_prod_marketing\".bookings.id FROM \"egdp_prod_marketing\".bookings");
    }

    @Test
    void testCatalogQualifiedColumnReferenceGetsPrefix()
    {
        assertThat(rewrite("SELECT hive.marketing.bookings.id FROM hive.marketing.bookings"))
                .isEqualTo("SELECT hive.\"egdp_prod_marketing\".bookings.id FROM hive.\"egdp_prod_marketing\".bookings");
    }

    @Test
    void testQualifiedColumnReferenceNotMatchingAnyRewrittenTableIsUnchanged()
    {
        // "alias.field" has two parts like a schema.table reference, but "alias" is a table alias, not a
        // schema, and no table named "marketing.field" was rewritten, so it must be left alone.
        assertThat(rewrite("SELECT alias.field FROM marketing.bookings AS alias"))
                .isEqualTo("SELECT alias.field FROM \"egdp_prod_marketing\".bookings AS alias");
    }

    @Test
    void testThreePartReferenceNotMatchingAnyRewrittenTableIsUnchanged()
    {
        // "unrelated.name.field" has three parts like a rewritten schema.table.column reference, but no
        // "unrelated.name" table was referenced (or rewritten) in this query, so it must be left alone.
        assertThat(rewrite("SELECT unrelated.name.field FROM marketing.bookings"))
                .isEqualTo("SELECT unrelated.name.field FROM \"egdp_prod_marketing\".bookings");
    }

    private static String rewrite(String sql)
    {
        return prefixSchemaNames(sql, PREFIX, ImmutableSet.of("hive"));
    }
}
