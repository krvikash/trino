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
package io.starburst.materialization.metastore.server;

import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mysql.MySQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMySqlRawMaterializationMetastore
        extends AbstractRawMaterializationMetastoreTest
{
    // A single utf8mb4 character is stored as up to 4 bytes.
    private static final String UTF8MB4_CHARACTER = "😀"; // U+1F600 GRINNING FACE

    @Override
    protected JdbcDatabaseContainer<?> createDbContainer()
    {
        return new MySQLContainer("mysql:8.0");
    }

    @Test
    public void testMaxLengthUtf8mb4IdentifiersRoundTrip()
    {
        // Fill the PK columns to (near) their maximum character counts with 4-byte utf8mb4 characters to prove
        // maximum-width multibyte identifiers are stored and read back intact. source_table is kept 9 characters below
        // its VARCHAR(256) size because definition() derives the storage table name as <table>_storage, which must fit
        // storage_table_name VARCHAR(256).
        RawMaterializationMetastore metastore = singleTenant(new MetastoreId(UTF8MB4_CHARACTER.repeat(128)));
        RawMaterializationDefinition definition = definition(
                UTF8MB4_CHARACTER.repeat(128),
                UTF8MB4_CHARACTER.repeat(256),
                UTF8MB4_CHARACTER.repeat(256));

        metastore.createOrReplace(definition);

        assertThat(metastore.listMaterializations()).containsExactly(definition);
    }
}
