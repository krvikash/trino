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
package io.trino.plugin.mongodb.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.plugin.mongodb.MongoClientConfig.SamplingOrder;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.mongodb.procedure.MongoTableProcedureId.UPDATE_SCHEMA;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class UpdateSchemaProcedure
        implements Provider<TableProcedureMetadata>
{
    public enum UpdateMode
    {
        /** Replace the table definition regardless of conflicts with the existing definition. */
        REPLACE,
        /** Throw an exception if the new definition conflicts with the existing definition. */
        FAIL,
    }

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                UPDATE_SCHEMA.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(integerProperty(
                                "sampling_count",
                                "How many documents are used for field type inference",
                                1,
                                value -> checkProcedureArgument(value > 0, "sampling_count must be positive: %s", value),
                                false))
                        .add(enumProperty(
                                "sampling_order",
                                "Which records should be read for sampling",
                                SamplingOrder.class,
                                SamplingOrder.FIRST,
                                false))
                        .add(enumProperty(
                                "mode",
                                "How to update the schema when there is a mismatch between existing schema and inferred schema",
                                UpdateMode.class,
                                UpdateMode.FAIL,
                                false))
                        .build());
    }
}
