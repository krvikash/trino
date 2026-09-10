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
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.KDB;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKdb
{
    private static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "kdb-product-test";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return ConnectorIdentity.ofUser("kdb-product-test");
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public Instant getStart()
        {
            return Instant.EPOCH;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property '%s' does not exist".formatted(name));
        }
    };

    @Test(groups = {KDB, PROFILE_SPECIFIC_TESTS})
    public void testSelect()
    {
        KdbClient client = new KdbClient(new KdbConnectionFactory(new StaticCredentialProvider(Optional.empty(), Optional.empty()), new KdbConfig().setHost("kdb")));
        client.execute(SESSION, "region:([]regionkey:`long$())");
        client.execute(SESSION, "`region upsert ([]regionkey:0 1 2 3 4j)");

        assertThat(onTrino().executeQuery("SELECT * FROM starburst_kdb.default.region"))
                .containsOnly(row(0), row(1), row(2), row(3), row(4));
    }
}
