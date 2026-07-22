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
package io.trino.plugin.deltalake.metastore;

import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record VendedCredentialsHandle(
        boolean catalogManaged,
        boolean managed,
        Optional<SchemaTableName> schemaTableName,
        String tableLocation)
{
    public VendedCredentialsHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(tableLocation, "tableLocation is null");

        if (catalogManaged) {
            checkArgument(managed, "Table must be managed by the catalog");
        }
        if (managed) {
            checkArgument(schemaTableName.isPresent(), "schemaTableName must be present for managed tables");
        }
    }

    public static VendedCredentialsHandle empty(String tableLocation)
    {
        return new VendedCredentialsHandle(false, false, Optional.empty(), tableLocation);
    }

    public static VendedCredentialsHandle of(DeltaMetastoreTable table)
    {
        return new VendedCredentialsHandle(
                table.catalogManaged(),
                table.managed(),
                Optional.of(table.schemaTableName()),
                table.location());
    }
}
