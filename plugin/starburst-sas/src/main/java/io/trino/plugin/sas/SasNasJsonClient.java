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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.sas.SasModule.ROOT_LOCATION_BINDING;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SasNasJsonClient
        extends SasNasClient
{
    private static final Logger log = Logger.get(SasNasJsonClient.class);
    private JsonSchemaRoot json;

    @Inject
    public SasNasJsonClient(@Named(ROOT_LOCATION_BINDING) Location rootLocation, SasConfig config, TrinoFileSystemFactory fileSystemFactory)
    {
        super(rootLocation, fileSystemFactory);
        if (config.getJsonFile().isPresent()) {
            File jsonFile = config.getJsonFile().get();
            try (InputStream is = Files.newInputStream(jsonFile.toPath())) {
                json = new ObjectMapper().readValue(is, JsonSchemaRoot.class);
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading JSON mapping file: " + jsonFile, e);
            }
            log.debug("Sas mapping: %s", json);
        }
    }

    @Override
    public List<String> getSchemaNames(ConnectorIdentity identity)
    {
        return json.listSchemas();
    }

    @Override
    public Set<String> getTableNames(String schema, ConnectorIdentity identity)
    {
        requireNonNull(schema, "schema is null");

        JsonMappingSchema mapSchemas = json.mapSchemas().get(schema.toLowerCase(ENGLISH));
        if (mapSchemas == null) {
            return ImmutableSet.of();
        }
        if (Strings.isNullOrEmpty(mapSchemas.path())) {
            return ImmutableSet.copyOf(mapSchemas.listTables());
        }
        return getTableNames(schema, mapSchemas.path(), identity);
    }

    @Override
    public Set<String> getTableNames(String schema, String dir, ConnectorIdentity identity)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        Location dirLocation = rootLocation.appendPath(stripLeadingSeparator(dir));
        checkFileSchema(schema);

        String dirPrefix = dirLocation.path() + "/";
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            FileIterator it = fileSystem.listFiles(dirLocation);
            while (it.hasNext()) {
                Location fileLocation = it.next().location();
                String fileName = fileLocation.fileName();
                // only immediate children; JSON mode does not recurse into subdirectories
                if (fileName.toLowerCase(ENGLISH).endsWith(SAS7BDAT_EXTENSION) && !fileLocation.path().substring(dirPrefix.length()).contains("/")) {
                    tables.add(stripSas7bdatExtension(fileName).toLowerCase(ENGLISH));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error listing tables in dir: " + dir, e);
        }
        return tables.build();
    }

    @Override
    public Optional<SasTable> getTable(String schema, String tableName, ConnectorIdentity identity)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        JsonMappingSchema beanSchema = json.mapSchemas().get(schema.toLowerCase(ENGLISH));
        if (beanSchema == null) {
            throw new SchemaNotFoundException(schema);
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
        Optional<Location> result;
        if (Strings.isNullOrEmpty(beanSchema.path())) {
            JsonMappingTable beanTable = beanSchema.mapTables().get(tableName.toLowerCase(ENGLISH));
            if (beanTable == null) {
                throw new TableNotFoundException(new SchemaTableName(schema, tableName));
            }
            try {
                result = findTable(fileSystem, rootLocation, beanTable.file());
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error looking up table: " + schema + "." + tableName, e);
            }
        }
        else {
            Location schemaPath = rootLocation.appendPath(stripLeadingSeparator(beanSchema.path()));
            try {
                result = findChildFileIgnoreCase(fileSystem, schemaPath, tableName);
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error looking up table: " + schema + "." + tableName, e);
            }
        }

        if (result.isEmpty()) {
            return Optional.empty();
        }
        Location location = result.get();
        checkFileTable(new SchemaTableName(schema, tableName));
        try (InputStream is = fileSystem.newInputFile(location).newStream()) {
            SasFileReaderImpl sasFileReader = new SasFileReaderImpl(is);
            long lineCount = sasFileReader.getSasFileProperties().getRowCount();
            long pageCount = sasFileReader.getSasFileProperties().getPageCount();
            List<SasColumn> columns = listColumnsFromFile(sasFileReader);
            return Optional.of(new SasTable(tableName, columns, location.toString(), lineCount, pageCount));
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading SAS file: " + location, e);
        }
    }

    private Optional<Location> findTable(TrinoFileSystem fileSystem, Location baseLocation, String tableFile)
            throws IOException
    {
        Location location = baseLocation.appendPath(stripLeadingSeparator(tableFile));
        if (!fileSystem.newInputFile(location).exists()) {
            return Optional.empty();
        }
        return Optional.of(location);
    }

    private static String stripLeadingSeparator(String path)
    {
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
