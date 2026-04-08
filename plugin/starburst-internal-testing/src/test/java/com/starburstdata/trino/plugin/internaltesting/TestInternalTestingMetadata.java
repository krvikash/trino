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
package com.starburstdata.trino.plugin.internaltesting;

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestInternalTestingMetadata
{
    private static final String OOM_SCHEMA_NAME = "oom";

    @Test
    void testContainsOOMSchemaName()
    {
        InternalTestingMetadata metadata = createMetadata();
        assertThat(metadata.listSchemaNames(SESSION)).contains(OOM_SCHEMA_NAME);
    }

    @Test
    void testOOMSchemaNameContainsWorkerAndCoordinatorPlace()
    {
        InternalTestingMetadata metadata = createMetadata();
        assertThat(metadata.listTables(SESSION, Optional.of(OOM_SCHEMA_NAME)))
                .containsExactlyInAnyOrder(
                        new SchemaTableName(OOM_SCHEMA_NAME, "coordinator"),
                        new SchemaTableName(OOM_SCHEMA_NAME, "worker"));
    }

    private static InternalTestingMetadata createMetadata()
    {
        return new InternalTestingMetadata(new InternalTestingMemoryAllocator());
    }
}
