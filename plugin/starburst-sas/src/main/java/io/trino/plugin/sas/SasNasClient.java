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
package io.trino.plugin.sas;

import com.epam.parso.impl.SasFileReaderImpl;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.sas.SasModule.ROOT_LOCATION_BINDING;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SasNasClient
        implements SasClient
{
    private static final Logger log = Logger.get(SasNasClient.class);
    // SAS7BDAT is the proprietary binary format used by SAS statistical software to store datasets
    protected static final String SAS7BDAT_EXTENSION = ".sas7bdat";
    protected final Location rootLocation;
    protected final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public SasNasClient(@Named(ROOT_LOCATION_BINDING) Location rootLocation, TrinoFileSystemFactory fileSystemFactory)
    {
        this.rootLocation = requireNonNull(rootLocation, "rootLocation is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public List<String> getSchemaNames(ConnectorIdentity identity)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        try {
            List<String> names = fileSystem.listDirectories(rootLocation).stream()
                    .map(location -> location.removeOneTrailingSlash().fileName().toLowerCase(ENGLISH))
                    .collect(toImmutableList());
            long distinctCount = names.stream().distinct().count();
            if (distinctCount != names.size()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Ambiguous schema names: two or more directories differ only by case");
            }
            return names;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error listing schemas", e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema, ConnectorIdentity identity)
    {
        requireNonNull(schema, "schema is null");
        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        try {
            Optional<Location> schemaDir = findChildDirectoryIgnoreCase(fileSystem, rootLocation, schema);
            if (schemaDir.isEmpty()) {
                return ImmutableSet.of();
            }
            Location schemaLocation = schemaDir.get();
            checkFileSchema(schema);
            String schemaDirPrefix = schemaLocation.path() + "/";

            LinkedHashMap<String, String> tableToFile = new LinkedHashMap<>();
            FileIterator it = fileSystem.listFiles(schemaLocation);
            while (it.hasNext()) {
                FileEntry entry = it.next();
                if (entry.location().fileName().toLowerCase(ENGLISH).endsWith(SAS7BDAT_EXTENSION)) {
                    String entryPath = entry.location().path();
                    if (entryPath.length() <= schemaDirPrefix.length()) {
                        continue;
                    }
                    String relativePath = entryPath.substring(schemaDirPrefix.length());
                    String tableName = stripSas7bdatExtension(relativePath).toLowerCase(ENGLISH);
                    String previous = tableToFile.put(tableName, relativePath);
                    if (previous != null) {
                        throw new TrinoException(
                                GENERIC_INTERNAL_ERROR,
                                "Ambiguous table name '%s' in schema '%s': matches both '%s' and '%s'".formatted(tableName, schema, previous, relativePath));
                    }
                }
            }
            return ImmutableSet.copyOf(tableToFile.keySet());
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error listing tables in schema: " + schema, e);
        }
    }

    @Override
    public Optional<SasTable> getTable(String schema, String tableName, ConnectorIdentity identity)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        Optional<Location> result = findTable(fileSystem, rootLocation, schema, tableName);
        checkFileTable(new SchemaTableName(schema, tableName));
        if (result.isEmpty()) {
            return Optional.empty();
        }
        Location tableLocation = result.get();
        try (InputStream is = fileSystem.newInputFile(tableLocation).newStream()) {
            SasFileReaderImpl sasFileReader = new SasFileReaderImpl(is);
            long lineCount = sasFileReader.getSasFileProperties().getRowCount();
            long pageCount = sasFileReader.getSasFileProperties().getPageCount();
            List<SasColumn> columns = listColumnsFromFile(sasFileReader);
            return Optional.of(new SasTable(tableName, columns, tableLocation.toString(), lineCount, pageCount));
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading SAS file: " + tableLocation, e);
        }
    }

    @Override
    public List<SasColumn> listColumnsFromFile(String filePath, TrinoFileSystem fileSystem)
    {
        try (InputStream is = fileSystem.newInputFile(Location.of(filePath)).newStream()) {
            return listColumnsFromFile(new SasFileReaderImpl(is));
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading SAS columns from file: " + filePath, e);
        }
    }

    protected Set<String> getTableNames(String schema, String dir, ConnectorIdentity identity)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        Location dirLocation = rootLocation.appendPath(schema).appendPath(dir);
        checkFileSchema(schema);

        String schemaDirPrefix = rootLocation.appendPath(schema).path() + "/";
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            FileIterator it = fileSystem.listFiles(dirLocation);
            while (it.hasNext()) {
                Location fileLocation = it.next().location();
                if (fileLocation.fileName().toLowerCase(ENGLISH).endsWith(SAS7BDAT_EXTENSION)) {
                    tables.add(stripSas7bdatExtension(fileLocation.path().substring(schemaDirPrefix.length())).toLowerCase(ENGLISH));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error listing tables in schema: " + schema + ", dir: " + dir, e);
        }
        return tables.build();
    }

    static void checkFileSchema(String schema)
    {
        if (schema.contains("..") || schema.contains("/") || schema.contains("\\")) {
            throw new SchemaNotFoundException(schema);
        }
    }

    static void checkFileTable(SchemaTableName table)
    {
        if (table.getTableName().contains("..")) {
            throw new TableNotFoundException(table);
        }
    }

    /**
     * Finds the file associated with the table case-insensitively.
     * Trino lowercases everything during query processing so lookups must be case-insensitive.
     */
    protected Optional<Location> findTable(TrinoFileSystem fileSystem, Location rootLocation, String schema, String tableName)
    {
        tableName = tableName.replace("\"", "");
        log.debug("findTable in: %s, schema: %s, tableName: %s", rootLocation, schema, tableName);
        try {
            Optional<Location> schemaDir = findChildDirectoryIgnoreCase(fileSystem, rootLocation, schema);
            if (schemaDir.isEmpty()) {
                return Optional.empty();
            }
            String[] parts = tableName.split("/");
            Location current = schemaDir.get();
            for (int i = 0; i < parts.length - 1; i++) {
                Optional<Location> next = findChildDirectoryIgnoreCase(fileSystem, current, parts[i]);
                if (next.isEmpty()) {
                    return Optional.empty();
                }
                current = next.get();
            }
            return findChildFileIgnoreCase(fileSystem, current, parts[parts.length - 1]);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error looking up table: " + schema + "." + tableName, e);
        }
    }

    protected Optional<Location> findChildFileIgnoreCase(TrinoFileSystem fileSystem, Location parent, String name)
            throws IOException
    {
        String prefix = parent.path() + "/";
        FileIterator it = fileSystem.listFiles(parent);
        List<Location> matches = new ArrayList<>();
        while (it.hasNext()) {
            Location fileLocation = it.next().location();
            // listFiles is recursive; skip entries deeper than immediate children
            if (fileLocation.path().substring(prefix.length()).contains("/")) {
                continue;
            }
            String fileName = fileLocation.fileName();
            if (!fileName.toLowerCase(ENGLISH).endsWith(SAS7BDAT_EXTENSION)) {
                continue;
            }
            if (name.equalsIgnoreCase(stripSas7bdatExtension(fileName))) {
                matches.add(fileLocation);
            }
        }
        if (matches.size() > 1) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Ambiguous table name: multiple files match '" + name + "' under " + parent);
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
    }

    protected Optional<Location> findChildDirectoryIgnoreCase(TrinoFileSystem fileSystem, Location parent, String name)
            throws IOException
    {
        List<Location> matches = new ArrayList<>();
        for (Location dir : fileSystem.listDirectories(parent)) {
            Location stripped = dir.removeOneTrailingSlash();
            if (name.equalsIgnoreCase(stripped.fileName())) {
                matches.add(stripped);
            }
        }
        if (matches.size() > 1) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Ambiguous schema name: multiple directories match '" + name + "' under " + parent);
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
    }

    protected static List<SasColumn> listColumnsFromFile(SasFileReaderImpl sasFileReader)
    {
        return sasFileReader.getColumns().stream()
                .map(SasColumn::getColumn)
                .collect(toImmutableList());
    }

    protected static String stripSas7bdatExtension(String fileName)
    {
        checkArgument(fileName.length() > SAS7BDAT_EXTENSION.length(), "fileName too short to contain extension %s: %s", SAS7BDAT_EXTENSION, fileName);
        return fileName.substring(0, fileName.length() - SAS7BDAT_EXTENSION.length());
    }
}
