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
package io.trino.plugin.bigquery.dynamic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner;
import io.trino.plugin.bigquery.TestBigQueryParentProjectId;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.CREDENTIALS_KEY_CREDENTIAL_NAME;
import static io.trino.plugin.bigquery.BigQueryDynamicConnectionQueryRunner.PARENT_PROJECT_ID_CREDENTIAL_NAME;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

final class TestBigQueryDynamicConnectionParentProjectId
        extends TestBigQueryParentProjectId
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final String TEST_SCHEMA = "test";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = BigQueryDynamicConnectionQueryRunner.builder()
                .amendSession(sessionBuilder -> sessionBuilder
                        .setIdentity(Identity.forUser("test_user")
                                .withExtraCredentials(ImmutableMap.of(
                                        PARENT_PROJECT_ID_CREDENTIAL_NAME, parentProjectId,
                                        CREDENTIALS_KEY_CREDENTIAL_NAME, requiredNonEmptySystemProperty("testing.bigquery.credentials-key")))
                                .build()))
                .build();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TPCH_SCHEMA);
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, ImmutableList.of(TpchTable.NATION));
        return queryRunner;
    }
}
