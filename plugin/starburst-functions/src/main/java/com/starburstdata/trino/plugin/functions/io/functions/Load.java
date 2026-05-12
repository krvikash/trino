/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io.functions;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.SchemaDiscoveryErrorCode;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.SchemaDiscoveryMappings;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.DescriptorArgumentSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugin.functions.FunctionsMetadata.IO_SCHEMA_NAME;
import static com.starburstdata.trino.plugin.functions.io.StorageLocations.checkLocationArgument;
import static io.trino.metastore.type.TypeConstants.STRING_TYPE_NAME;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Load
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "load";

    private static final String LOCATION_ARGUMENT_NAME = "LOCATION";
    private static final String FORMAT_ARGUMENT_NAME = "FORMAT";
    private static final String DESCRIPTOR_ARGUMENT_NAME = "COLUMNS";
    private static final String SKIP_HEADER_ARGUMENT_NAME = "SKIP_HEADER";
    private static final String FIELD_SEPARATOR_ARGUMENT_NAME = "FIELD_SEPARATOR";
    private static final String QUOTE_CHAR_ARGUMENT_NAME = "QUOTE_CHAR";
    private static final String ESCAPE_CHAR_ARGUMENT_NAME = "ESCAPE_CHAR";

    // Make timeout configurable in the future if needed
    private static final Integer SCHEMA_DISCOVERY_TIMEOUT_SECONDS = 30;

    private final TypeManager typeManager;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final LocationAccessControl locationAccessControl;

    @Inject
    public Load(TypeManager typeManager, TrinoFileSystemFactory fileSystemFactory, LocationAccessControl locationAccessControl)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new LoadFunction(), getClass().getClassLoader());
    }

    public class LoadFunction
            extends AbstractConnectorTableFunction
    {
        public LoadFunction()
        {
            super(
                    IO_SCHEMA_NAME,
                    NAME,
                    ImmutableList.<ArgumentSpecification>builder()
                            .add(ScalarArgumentSpecification.builder()
                                    .name(LOCATION_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name(FORMAT_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build())
                            .add(DescriptorArgumentSpecification.builder()
                                    .name(DESCRIPTOR_ARGUMENT_NAME)
                                    .defaultValue(null)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name(SKIP_HEADER_ARGUMENT_NAME)
                                    .type(INTEGER)
                                    .defaultValue(null)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name(FIELD_SEPARATOR_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name(QUOTE_CHAR_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name(ESCAPE_CHAR_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build())
                            .build(),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            ScalarArgument locationArgument = (ScalarArgument) arguments.get(LOCATION_ARGUMENT_NAME);
            ScalarArgument formatArgument = (ScalarArgument) arguments.get(FORMAT_ARGUMENT_NAME);
            DescriptorArgument descriptorArgument = (DescriptorArgument) arguments.get(DESCRIPTOR_ARGUMENT_NAME);
            ScalarArgument headerArgument = (ScalarArgument) arguments.get(SKIP_HEADER_ARGUMENT_NAME);
            ScalarArgument fieldSeparatorArgument = (ScalarArgument) arguments.get(FIELD_SEPARATOR_ARGUMENT_NAME);
            ScalarArgument quoteCharArgument = (ScalarArgument) arguments.get(QUOTE_CHAR_ARGUMENT_NAME);
            ScalarArgument escapeArgument = (ScalarArgument) arguments.get(ESCAPE_CHAR_ARGUMENT_NAME);
            OptionalInt skipHeader = headerArgument.getNullableValue().isNull() ? OptionalInt.empty() : OptionalInt.of(((Number) headerArgument.getValue()).intValue());
            Optional<Character> fieldSeparator = getSingleCharacter(FIELD_SEPARATOR_ARGUMENT_NAME, fieldSeparatorArgument);
            Optional<Character> quote = getSingleCharacter(QUOTE_CHAR_ARGUMENT_NAME, quoteCharArgument);
            Optional<Character> escape = getSingleCharacter(ESCAPE_CHAR_ARGUMENT_NAME, escapeArgument);
            checkFunctionArgument(
                    formatArgument.getNullableValue().isNull() == descriptorArgument.getDescriptor().isEmpty(),
                    "%s and %s arguments must be both specified or both omitted", FORMAT_ARGUMENT_NAME, DESCRIPTOR_ARGUMENT_NAME);

            checkFunctionArgument(locationArgument.getValue() != null, "location cannot be null");
            String location = ((Slice) locationArgument.getValue()).toStringUtf8();
            boolean isDirectory = location.endsWith("/");
            location = stripTrailingSlash(location);

            checkLocationArgument(location);
            locationAccessControl.checkCanUseLocation(session.getIdentity(), location);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            LoadTableHandle tableHandle;
            if (formatArgument.getNullableValue().isNull()) {
                tableHandle = withSchemaDiscovery(fileSystem, location, skipHeader, fieldSeparator, quote, escape);
            }
            else {
                tableHandle = withDescriptor(location, isDirectory, ((Slice) formatArgument.getValue()).toStringUtf8(), descriptorArgument.getDescriptor().orElseThrow().getFields(), skipHeader, fieldSeparator, quote, escape);
            }

            Descriptor returnedType = new Descriptor(tableHandle.columns.stream()
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            LoadFunctionHandle handle = new LoadFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }

        private LoadTableHandle withSchemaDiscovery(
                TrinoFileSystem fileSystem,
                String location,
                OptionalInt skipHeader,
                Optional<Character> fieldSeparator,
                Optional<Character> quote,
                Optional<Character> escape)
        {
            SchemaDiscoveryController controller = createSchemaDiscoveryController(fileSystem);
            ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
            fieldSeparator.ifPresent(value -> options.put(CsvOptions.DELIMITER, String.valueOf(value)));
            quote.ifPresent(value -> options.put(CsvOptions.QUOTE, String.valueOf(value)));
            escape.ifPresent(value -> options.put(CsvOptions.ESCAPE, String.valueOf(value)));
            ListenableFuture<DiscoveredSchema> guess = controller.guess(new GuessRequest(URI.create(location), options.buildOrThrow()));

            DiscoveredSchema discoveredSchema;
            try {
                discoveredSchema = guess.get(SCHEMA_DISCOVERY_TIMEOUT_SECONDS, SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new TrinoException(SchemaDiscoveryErrorCode.IO, e);
            }
            List<DiscoveredTable> tables = discoveredSchema.tables();
            if (tables.isEmpty()) {
                throw new TrinoException(NOT_FOUND, "Table not found at location: " + location);
            }
            if (tables.size() > 1) {
                throw new TrinoException(GENERIC_USER_ERROR, "Multiple tables found at location: " + location);
            }
            DiscoveredTable discoveredTable = tables.getFirst();
            List<Column> discoveredColumns = discoveredTable.columns().columns();

            List<HiveColumnHandle> columns = IntStream.range(0, discoveredColumns.size())
                    .mapToObj(i -> toHiveColumn(discoveredTable.format(), discoveredColumns.get(i), i))
                    .collect(toImmutableList());

            HiveStorageFormat format = HiveStorageFormat.valueOf(SchemaDiscoveryMappings.tableFormat(discoveredTable));
            return new LoadTableHandle(location, true, format, columns, skipHeader, fieldSeparator, quote, escape);
        }

        private HiveColumnHandle toHiveColumn(TableFormat format, Column column, int index)
        {
            return new HiveColumnHandle(
                    column.name().string(),
                    index,
                    HiveType.valueOf(format == TableFormat.CSV ? STRING_TYPE_NAME : column.type().typeInfo().getTypeName()),
                    typeManager.fromSqlType(format == TableFormat.CSV ? "varchar" : SchemaDiscoveryMappings.sqlType(column)),
                    Optional.empty(),
                    REGULAR,
                    Optional.empty());
        }

        private static LoadTableHandle withDescriptor(
                String location,
                boolean isDirectory,
                String formatValue,
                List<Descriptor.Field> fields,
                OptionalInt skipHeader,
                Optional<Character> fieldSeparator,
                Optional<Character> quote,
                Optional<Character> escape)
        {
            HiveStorageFormat format = Enums.getIfPresent(HiveStorageFormat.class, formatValue.toUpperCase(ENGLISH)).toJavaUtil()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, formatValue + " format isn't supported"));
            List<HiveColumnHandle> columnHandles = IntStream.range(0, fields.size()).mapToObj(i -> toHiveColumn(fields.get(i), i)).collect(toImmutableList());
            return new LoadTableHandle(location, isDirectory, format, columnHandles, skipHeader, fieldSeparator, quote, escape);
        }

        private static HiveColumnHandle toHiveColumn(Descriptor.Field field, int index)
        {
            String name = field.getName().orElseThrow();
            Type type = field.getType().orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "Type is required for column: " + name));
            return createBaseColumn(
                    name,
                    index,
                    toHiveType(type),
                    type,
                    REGULAR,
                    Optional.empty());
        }

        private static SchemaDiscoveryController createSchemaDiscoveryController(TrinoFileSystem fileSystem)
        {
            Function<URI, DiscoveryTrinoFileSystem> fileSystemProvider = _ -> new DiscoveryTrinoFileSystem(fileSystem);
            OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputFile) -> new HdfsOrcDataSource(id, size, options, inputFile, new FileFormatDataSourceStats());
            ParquetDataSourceFactory parquetDataSourceFactory = (inputFile) -> new TrinoParquetDataSource(inputFile, ParquetReaderOptions.defaultOptions(), new FileFormatDataSourceStats());
            return new SchemaDiscoveryController(fileSystemProvider, parquetDataSourceFactory, orcDataSourceFactory, Dialect.TRINO);
        }

        private static String stripTrailingSlash(String path)
        {
            String result = path;
            if (path.endsWith("/")) {
                result = result.substring(0, result.length() - 1);
            }
            checkFunctionArgument(!result.endsWith("/"), "Multiple trailing slashes are not supported: '%s'", path);

            return result;
        }

        private static Optional<Character> getSingleCharacter(String key, ScalarArgument argument)
        {
            if (argument.getNullableValue().isNull()) {
                return Optional.empty();
            }
            Slice slice = (Slice) argument.getNullableValue().getValue();
            String value = slice.toStringUtf8();
            checkFunctionArgument(value.length() == 1, "%s must be a single character string, but was: '%s'", key, value);
            return Optional.of(value.charAt(0));
        }
    }

    public record LoadFunctionHandle(LoadTableHandle tableHandle)
            implements ConnectorTableFunctionHandle
    {
        public LoadFunctionHandle
        {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }

    public record LoadTableHandle(
            String location,
            boolean isDirectory,
            HiveStorageFormat format,
            List<HiveColumnHandle> columns,
            OptionalInt skipHeader,
            Optional<Character> fieldSeparator,
            Optional<Character> quote,
            Optional<Character> escape)
            implements ConnectorTableHandle
    {
        public LoadTableHandle
        {
            requireNonNull(location, "location is null");
            requireNonNull(format, "format is null");
            columns = ImmutableList.copyOf(columns);
            requireNonNull(skipHeader, "skipHeader is null");
            requireNonNull(fieldSeparator, "fieldSeparator is null");
            requireNonNull(quote, "quote is null");
            requireNonNull(escape, "escape is null");

            if (skipHeader.isPresent()) {
                checkFunctionArgument(skipHeader.getAsInt() >= 0, "%s must be >= 0", SKIP_HEADER_ARGUMENT_NAME);
                checkFunctionArgument(Set.of(TEXTFILE, CSV).contains(format), "Cannot specify header for storage format: %s", format);
            }
            if (fieldSeparator.isPresent()) {
                checkFunctionArgument(format == CSV, "Cannot specify field separator for storage format: %s", format);
            }
            if (quote.isPresent()) {
                checkFunctionArgument(format == CSV, "Cannot specify quote for storage format: %s", format);
            }
            if (escape.isPresent()) {
                checkFunctionArgument(format == CSV, "Cannot specify escape for storage format: %s", format);
            }
        }
    }
}
