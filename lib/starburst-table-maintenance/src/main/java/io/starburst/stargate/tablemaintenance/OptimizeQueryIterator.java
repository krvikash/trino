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
package io.starburst.stargate.tablemaintenance;

import io.airlift.units.DataSize;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.starburst.stargate.tablemaintenance.MaintenanceQueryGenerators.buildOptimizeTable;
import static java.util.Objects.requireNonNull;

public class OptimizeQueryIterator
        implements Iterator<String>
{
    private final List<String> optimizeCheckpointPredicates;
    private final CatalogSchemaTableName tableName;
    private final DataSize optimizeFileThreshold;
    private int currentIndex;

    public OptimizeQueryIterator(
            CatalogSchemaTableName catalogSchemaTableName,
            List<String> optimizeCheckpointPredicates,
            DataSize optimizeFileThreshold)
    {
        this.tableName = requireNonNull(catalogSchemaTableName, "catalogSchemaTableName is null");
        this.optimizeCheckpointPredicates = requireNonNull(optimizeCheckpointPredicates, "optimizeCheckpointPredicates is null");
        this.optimizeFileThreshold = requireNonNull(optimizeFileThreshold, "optimizeFileThreshold is null");
    }

    @Override
    public boolean hasNext()
    {
        return currentIndex < optimizeCheckpointPredicates.size();
    }

    @Override
    public String next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException("No more optimize queries");
        }
        String predicate = optimizeCheckpointPredicates.get(currentIndex);
        currentIndex++;
        return buildOptimizeTable(tableName, optimizeFileThreshold, predicate);
    }

    public int totalCount()
    {
        return optimizeCheckpointPredicates.size();
    }
}
