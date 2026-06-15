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

import org.apache.iceberg.Table;

import static java.util.Objects.requireNonNull;

/**
 * Holds an Iceberg {@link Table} together with the cleanup action (catalog
 * close, etc.) that must run when the consumer is done.
 */
public record TableContext(Table table, Runnable cleanup)
        implements AutoCloseable
{
    public TableContext
    {
        requireNonNull(table, "table is null");
        requireNonNull(cleanup, "cleanup is null");
    }

    public static TableContext of(Table table, IcehouseCatalog catalog)
    {
        return new TableContext(table, catalog::close);
    }

    public static TableContext of(Table table)
    {
        return new TableContext(table, () -> {});
    }

    @Override
    public void close()
    {
        cleanup.run();
    }
}
