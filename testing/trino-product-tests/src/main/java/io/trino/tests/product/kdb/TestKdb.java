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
package io.trino.tests.product.kdb;

import com.starburstdata.plugin.kdb.KdbClient;
import com.starburstdata.plugin.kdb.KdbConfig;
import com.starburstdata.plugin.kdb.KdbConnectionFactory;
import com.starburstdata.plugin.kdb.KdbCredentialConfig;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.KDB;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKdb
{
    @Test(groups = {KDB, PROFILE_SPECIFIC_TESTS})
    public void testSelect()
    {
        KdbClient client = new KdbClient(new KdbConnectionFactory(new KdbConfig().setHost("kdb"), new KdbCredentialConfig()));
        client.execute("region:([]regionkey:`long$())");
        client.execute("`region upsert ([]regionkey:0 1 2 3 4j)");

        assertThat(onTrino().executeQuery("SELECT * FROM starburst_kdb.default.region"))
                .containsOnly(row(0), row(1), row(2), row(3), row(4));
    }
}
