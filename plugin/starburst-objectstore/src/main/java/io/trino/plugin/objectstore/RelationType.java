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

import java.util.Optional;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.Objects.requireNonNull;

public enum RelationType
{
    MATERIALIZED_VIEW(Optional.empty()),
    VIEW(Optional.empty()),
    HIVE_TABLE(Optional.of(HIVE)),
    ICEBERG_TABLE(Optional.of(ICEBERG)),
    DELTA_TABLE(Optional.of(DELTA)),
    HUDI_TABLE(Optional.of(HUDI)),
    /**/;

    public static RelationType from(TableType tableType)
    {
        return switch (tableType) {
            case HIVE -> HIVE_TABLE;
            case ICEBERG -> ICEBERG_TABLE;
            case DELTA -> DELTA_TABLE;
            case HUDI -> HUDI_TABLE;
        };
    }

    private final Optional<TableType> tableType;

    RelationType(Optional<TableType> tableType)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    public Optional<TableType> tableType()
    {
        return tableType;
    }
}
