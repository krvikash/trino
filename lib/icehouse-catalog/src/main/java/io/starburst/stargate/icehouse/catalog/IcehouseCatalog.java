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
package io.starburst.stargate.icehouse.catalog;

import io.starburst.stargate.icehouse.exception.RetryableIcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.starburst.stargate.icehouse.spi.TableIdentifier;
import org.apache.iceberg.Table;

import java.util.List;

/**
 * Facade over an Iceberg-compatible catalog backend used by icehouse maintenance.
 * Stateful (holds open clients), bound to a single {@code (accountId, catalogId)}
 * tuple, not thread-safe. Obtain via {@link IcehouseCatalogFactory#create} and
 * close when done.
 */
public interface IcehouseCatalog
        extends AutoCloseable
{
    /**
     * @throws RetryableIcehouseCatalogException on transient failures
     * @throws TerminalIcehouseCatalogException on permanent failures
     */
    List<String> listSchemas();

    /**
     * Lists Iceberg tables in the given schema, filtering out non-Iceberg objects.
     *
     * @throws RetryableIcehouseCatalogException on transient failures
     * @throws TerminalIcehouseCatalogException on permanent failures
     */
    List<String> listTables(String schema);

    /**
     * Loads a table. The returned {@link Table} carries its own {@code TableOperations}
     * and {@code FileIO}.
     *
     * @throws TableNotFoundException if the table does not exist
     * @throws RetryableIcehouseCatalogException on transient failures
     * @throws TerminalIcehouseCatalogException on permanent failures
     */
    Table loadTable(TableIdentifier tableId);

    /**
     * Returns the table's current metadata file location without setting up FileIO.
     *
     * @throws TableNotFoundException if the table does not exist
     * @throws RetryableIcehouseCatalogException on transient failures
     * @throws TerminalIcehouseCatalogException on permanent failures
     */
    String metadataLocation(TableIdentifier tableId);

    @Override
    void close();
}
