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
package io.trino.plugin.deltalake.transactionlog.reader;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.hive.metastore.unity.UnityMetastoreConfig;
import io.trino.spi.TrinoException;

import static io.trino.plugin.deltalake.DeltaLakeMetadata.isCatalogOwnedTable;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class UnityTransactionLogReaderFactory
        implements TransactionLogReaderFactory
{
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final DeltaLakeTableOperationsProvider tableOperationsProvider;
    private final boolean isCatalogOwnedTableEnabled;

    @Inject
    public UnityTransactionLogReaderFactory(
            DeltaLakeFileSystemFactory fileSystemFactory,
            DeltaLakeTableOperationsProvider tableOperationsProvider,
            UnityMetastoreConfig unityMetastoreConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.isCatalogOwnedTableEnabled = unityMetastoreConfig.isCatalogOwnedTableEnabled();
    }

    @Override
    public TransactionLogReader createReader(DeltaLakeTableHandle tableHandle)
    {
        if (isCatalogOwnedTable(tableHandle.getProtocolEntry())) {
            if (!isCatalogOwnedTableEnabled) {
                throw new TrinoException(NOT_SUPPORTED, "Catalog owned table not enabled");
            }
            String tableId = tableHandle.getMetadataEntry().getTableId().orElseThrow(() -> new IllegalArgumentException("Table id is required for Unity Catalog owned tables"));
            return new RestUnityTransactionLogReader(tableId, tableHandle.getLocation(), tableHandle.toCredentialsHandle(), fileSystemFactory, tableOperationsProvider);
        }

        return new FileSystemTransactionLogReader(tableHandle.getLocation(), tableHandle.toCredentialsHandle(), fileSystemFactory);
    }

    @Override
    public TransactionLogReader createReader(DeltaMetastoreTable table)
    {
        VendedCredentialsHandle credentialsHandle = VendedCredentialsHandle.of(table);
        if (table.catalogOwned()) {
            if (!isCatalogOwnedTableEnabled) {
                throw new TrinoException(NOT_SUPPORTED, "Catalog owned table not enabled");
            }
            String tableId = table.tableId().orElseThrow(() -> new IllegalArgumentException("Table id is required for Unity Catalog owned tables"));
            return new RestUnityTransactionLogReader(tableId, table.location(), credentialsHandle, fileSystemFactory, tableOperationsProvider);
        }

        return new FileSystemTransactionLogReader(table.location(), credentialsHandle, fileSystemFactory);
    }
}
