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
package io.trino.plugin.objectstore.functions.unload;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ObjectStoreUnloadFunction
        extends AbstractConnectorTableFunction
{
    private static final String TABLE_ARGUMENT_NAME = "INPUT";
    private static final String LOCATION_ARGUMENT_NAME = "LOCATION";
    private static final String FORMAT_ARGUMENT_NAME = "FORMAT";
    private static final String COMPRESSION_ARGUMENT_NAME = "COMPRESSION";
    private static final String SEPARATOR_ARGUMENT_NAME = "SEPARATOR";
    private static final String HEADER_ARGUMENT_NAME = "HEADER";
    private static final String EXISTING_DIRECTORY_ARGUMENT_NAME = "EXISTING_DIRECTORY";

    private final ConnectorTableFunction unload;

    public ObjectStoreUnloadFunction(Connector hiveConnector)
    {
        super(
                "system",
                "unload",
                ImmutableList.of(
                        TableArgumentSpecification.builder()
                                .name(TABLE_ARGUMENT_NAME)
                                .pruneWhenEmpty()
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(LOCATION_ARGUMENT_NAME)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(FORMAT_ARGUMENT_NAME)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(COMPRESSION_ARGUMENT_NAME)
                                .type(VARCHAR)
                                .defaultValue(utf8Slice("NONE"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(SEPARATOR_ARGUMENT_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(HEADER_ARGUMENT_NAME)
                                .type(BOOLEAN)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(EXISTING_DIRECTORY_ARGUMENT_NAME)
                                .type(VARCHAR)
                                .defaultValue(utf8Slice("CHECK"))
                                .build()),
                new ReturnTypeSpecification.DescribedTable(descriptor(ImmutableList.of("path", "count"), ImmutableList.of(VARCHAR, BIGINT))));
        this.unload = hiveConnector
                .getTableFunctions().stream()
                .filter(connectorTableFunction -> connectorTableFunction.getName().equals("unload"))
                .collect(onlyElement());
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl,
            RetryMode retryMode)
    {
        return unload.analyze(session, transaction, arguments, accessControl, retryMode);
    }
}
