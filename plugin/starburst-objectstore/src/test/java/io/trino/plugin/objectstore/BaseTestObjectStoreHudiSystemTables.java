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
package io.trino.plugin.objectstore;

import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.Test;

public abstract class BaseTestObjectStoreHudiSystemTables
        extends AbstractTestQueryFramework
{
    @Test
    public void testTimelineTable()
    {
        assertQuery("SHOW COLUMNS FROM tpch.\"nation$timeline\"",
                "VALUES ('timestamp', 'varchar', '', '')," +
                        "('action', 'varchar', '', '')," +
                        "('state', 'varchar', '', '')");

        // timestamp column isn't static
        assertQuery("SELECT action, state FROM tpch.\"nation$timeline\"",
                "VALUES ('commit', 'COMPLETED')");

        assertQueryFails("SELECT timestamp, action, state FROM tpch.\"orders$timeline\"",
                ".*Table 'objectstore.tpch.\"orders\\$timeline\"' does not exist");
    }
}
