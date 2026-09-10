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
package com.starburstdata.plugin.kdb;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.TemporaryRelation;

public class KdbTemporaryTable
        implements TemporaryRelation
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setIdentity(ConnectorIdentity.ofUser("kdb-temporary-table"))
            .build();

    private final KdbClient kdbClient;
    private final String name;

    KdbTemporaryTable(KdbClient kdbClient, String name)
    {
        this.kdbClient = kdbClient;
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        try {
            kdbClient.execute(SESSION, "delete " + name + " from `.");
        }
        catch (Exception ignored) {
            // cleanup failed
        }
    }
}
