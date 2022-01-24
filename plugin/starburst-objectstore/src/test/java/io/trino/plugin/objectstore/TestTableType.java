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

import org.junit.jupiter.api.Test;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableType
{
    @Test
    public void testCanonicalOrder()
    {
        // ObjectStore logic may depend on the ordering of these, e.g. which property description gets used, etc.
        assertThat(TableType.values())
                .containsExactly(HIVE, ICEBERG, DELTA, HUDI);
    }
}
