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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query-level coverage for multi-split reads. The {@code sas_fine_split} catalog sets
 * {@code sas.min-page-per-split=1} so the small test files genuinely divide into one split per
 * page; the plain table name (catalog default {@code sas.split-count=1}) is the single-split
 * baseline every {@code "table$N"} override is compared against.
 *
 * <p>Regression coverage for the split page-accounting bug: splits with {@code start > 0} used to
 * fail with "Error reading SAS record" on compressed files (page-range budget spent by the skip
 * phase, then {@code readNext} dereferenced an empty subheader-pointer list) and could silently
 * drop or duplicate page rows on uncompressed files.
 */
final class TestSasSplitQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path dataDirectory = Path.of("src/test/resources").toAbsolutePath();
        DistributedQueryRunner queryRunner = SasQueryRunner.builder(dataDirectory).build();
        queryRunner.createCatalog("sas_fine_split", "sas", ImmutableMap.of(
                "sas.data-directory", dataDirectory.toUri().toString(),
                "sas.min-page-per-split", "1"));
        return queryRunner;
    }

    @Test
    void testCompressedFileSplitCounts()
    {
        // param_gen_nn1 is SASYZCRL-compressed with 3 pages; every split override must see all 328 rows
        assertThat(query("SELECT count(*) FROM sas_fine_split.prod.param_gen_nn1"))
                .matches("VALUES BIGINT '328'");
        for (int splits = 1; splits <= 5; splits++) {
            assertThat(query("SELECT count(*) FROM sas_fine_split.prod.\"param_gen_nn1$%s\"".formatted(splits)))
                    .matches("VALUES BIGINT '328'");
        }
    }

    @Test
    void testCompressedFileSplitDataMatchesSingleSplit()
    {
        assertThat(query("SELECT * FROM sas_fine_split.prod.\"param_gen_nn1$3\""))
                .matches("SELECT * FROM sas_fine_split.prod.param_gen_nn1");
    }

    @Test
    void testUncompressedFileSplitCounts()
    {
        // colon is uncompressed with 7 pages; wrong page accounting used to drop rows silently
        assertThat(query("SELECT count(*) FROM sas_fine_split.schema1.colon"))
                .matches("VALUES BIGINT '15564'");
        for (int splits : new int[] {2, 3, 7}) {
            assertThat(query("SELECT count(*) FROM sas_fine_split.schema1.\"colon$%s\"".formatted(splits)))
                    .matches("VALUES BIGINT '15564'");
        }
    }

    @Test
    void testUncompressedFileSplitDataMatchesSingleSplit()
    {
        assertThat(query("SELECT * FROM sas_fine_split.schema1.\"colon$7\""))
                .matches("SELECT * FROM sas_fine_split.schema1.colon");
    }

    @Test
    void testUncompressedFileSplitAggregationMatchesSingleSplit()
    {
        assertThat(query("SELECT sum(age), sum(surv_mm) FROM sas_fine_split.schema1.\"colon$7\""))
                .matches("SELECT sum(age), sum(surv_mm) FROM sas_fine_split.schema1.colon");
    }

    @Test
    void testSplitCountLargerThanPageCount()
    {
        // pd_marg has a single page: any override collapses to one split
        assertThat(query("SELECT count(*) FROM sas_fine_split.prod.\"pd_marg$5\""))
                .matches("SELECT count(*) FROM sas_fine_split.prod.pd_marg");
        assertThat(query("SELECT count(*) FROM sas_fine_split.schema1.\"colon$100\""))
                .matches("VALUES BIGINT '15564'");
    }

    @Test
    void testMinPagePerSplitCollapsesSplitOverride()
    {
        // the default catalog keeps sas.min-page-per-split=2000, so $N on a small file stays one split
        assertThat(query("SELECT count(*) FROM sas.prod.\"param_gen_nn1$3\""))
                .matches("VALUES BIGINT '328'");
    }
}
