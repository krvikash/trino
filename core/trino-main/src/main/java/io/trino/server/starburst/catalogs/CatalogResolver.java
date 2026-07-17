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
package io.trino.server.starburst.catalogs;

import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.trino.spi.connector.SchemaTableName;
import io.trino.transaction.TransactionId;

import java.util.Optional;
import java.util.Set;

public interface CatalogResolver
{
    boolean isReadOnlyCatalog(Optional<TransactionId> transactionId, String catalogName);

    Optional<CatalogId> getCatalogId(Optional<TransactionId> transactionId, String catalogName);

    Optional<String> getCatalogName(Optional<TransactionId> transactionId, CatalogId catalogId);

    Optional<SharedSchemaNameAndAccepted> getSharedSchemaForCatalog(Optional<TransactionId> transactionId, String catalogName);

    default Optional<String> getCatalogName(TransactionId transactionId, CatalogId catalogId)
    {
        return getCatalogName(Optional.of(transactionId), catalogId);
    }

    Set<SchemaTableName> getAlwaysVisibleSystemTables(Optional<TransactionId> transactionId, String catalogName);
}
