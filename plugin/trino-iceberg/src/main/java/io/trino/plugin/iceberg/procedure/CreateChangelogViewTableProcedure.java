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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;

import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.CREATE_CHANGELOG_VIEW;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class CreateChangelogViewTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                CREATE_CHANGELOG_VIEW.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(longProperty(
                                "start_snapshot_id",
                                "Start snapshot ID",
                                null,
                                false))
                        .add(longProperty(
                                "end_snapshot_id",
                                "End snapshot ID",
                                null,
                                false))
                        .add(new PropertyMetadata<>(
                                "identifier_columns",
                                "Columns used to identify rows for change tracking",
                                new ArrayType(VARCHAR),
                                List.class,
                                null,
                                false,
                                value -> (List<?>) value,
                                value -> value))
                        .add(stringProperty(
                                "view_name",
                                "Name of the view to create; defaults to {table_name}_changes",
                                null,
                                false))
                        .add(stringProperty(
                                "schema_name",
                                "Schema in which to create the view; defaults to the source table's schema",
                                null,
                                false))
                        .add(booleanProperty(
                                "skip_write_mode_validation",
                                "Skip the check that the table uses copy-on-write write modes",
                                false,
                                true))
                        .build());
    }
}
