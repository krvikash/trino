/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor;
import io.trino.filesystem.Location;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.schema.discovery.Util.schemaDiscoveryInstances;
import static io.starburst.schema.discovery.Util.toLocalDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSchemaDiscovery
{
    private static final OptionsMap OPTIONS = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1"));

    @Test
    public void testIceberg(@TempDir Path tempDir)
            throws IOException
    {
        Location tableLocation = prepareTestTableWithCorrectLocation(tempDir, "table1");
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), tableLocation, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1_000))
                .matches(discovered -> discovered.rootPath().path().endsWith("table1/")
                        && discovered.errors().isEmpty()
                        && discovered.tables().size() == 1)
                .extracting(discovered -> getOnlyElement(discovered.tables()))
                .matches(table -> table.valid() && table.path().path().equals(tableLocation.toString()) && table.format() == TableFormat.ICEBERG);
    }

    @Test
    public void testIcebergTableWithoutLocationInMetadata(@TempDir Path tempDir)
            throws IOException
    {
        prepareTestInvalidTable(tempDir);
        Location directory = Util.toLocalDirectory(tempDir);

        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().equals(directory + "/")
                        && discovered.errors().isEmpty()
                        && discovered.tables().size() == 1)
                .extracting(discovered -> getOnlyElement(discovered.tables()))
                .matches(table -> !table.valid(), "Table must be invalid")
                .matches(table -> !table.errors().isEmpty() && table.errors().getFirst().equals("Failed to read iceberg table's latest metadata file - [Cannot parse missing string: location]"), "Table must have invalid metadata error")
                .matches(table -> table.format() == TableFormat.ERROR, "Table must have error format");
    }

    @Test
    public void testIcebergTablesParent(@TempDir Path tempDir)
            throws IOException
    {
        prepareTestTableWithCorrectLocation(tempDir, "table1");
        prepareTestTableWithCorrectLocation(tempDir, "table2");
        prepareTestInvalidTable(tempDir);
        Location directory = Util.toLocalDirectory(tempDir);

        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().equals(directory + "/")
                        && discovered.tables().size() == 3)
                .extracting(discoveredSchema -> discoveredSchema.tables().stream()
                        .sorted(Comparator.comparing(DiscoveredTable::path))
                        .collect(toImmutableList()))
                .satisfies(tables -> assertThat(tables)
                        .anyMatch(table -> !table.valid() && !table.errors().isEmpty() && table.format() == TableFormat.ERROR)
                        .anyMatch(table -> table.valid() && table.path().path().equals(directory.appendPath("table1").toString()) && table.format() == TableFormat.ICEBERG)
                        .anyMatch(table -> table.valid() && table.path().path().equals(directory.appendPath("table2").toString()) && table.format() == TableFormat.ICEBERG));
    }

    @Test
    public void testIcebergTablesParentWithExclude(@TempDir Path tempDir)
            throws IOException
    {
        Path icebergDirectory = tempDir.resolve("iceberg");
        icebergDirectory.toFile().mkdir();
        prepareTestTableWithCorrectLocation(icebergDirectory, "table1");
        prepareTestTableWithCorrectLocation(icebergDirectory, "table2");
        prepareTestInvalidTable(icebergDirectory);
        Location directory = Util.toLocalDirectory(icebergDirectory);

        OptionsMap options = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.EXCLUDE_PATTERNS, "**/{iceberg/table1/}*"));
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, options, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().equals(directory + "/")
                        && discovered.tables().size() == 2)
                .extracting(discoveredSchema -> discoveredSchema.tables().stream()
                        .sorted(Comparator.comparing(DiscoveredTable::path))
                        .collect(toImmutableList()))
                .satisfies(tables -> assertThat(tables)
                        .anyMatch(table -> !table.valid() && !table.errors().isEmpty() && table.format() == TableFormat.ERROR)
                        .anyMatch(table -> table.valid() && table.path().path().equals(directory.appendPath("table2").toString()) && table.format() == TableFormat.ICEBERG));
    }

    @Test
    public void testModuloRecursiveIceberg(@TempDir Path tempDir)
            throws IOException
    {
        prepareTestTableWithCorrectLocation(tempDir, "table1");
        prepareTestTableWithCorrectLocation(tempDir, "table2");
        prepareTestInvalidTable(tempDir);
        Location directory = Util.toLocalDirectory(tempDir);

        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "8", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1", GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), directory, optionsMap, Executors.newCachedThreadPool());
        processor.startRootProcessing();
        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().equals(directory + "/")
                        && discovered.tables().size() == 3)
                .extracting(discoveredSchema -> discoveredSchema.tables().stream()
                        .sorted(Comparator.comparing(DiscoveredTable::path))
                        .collect(toImmutableList()))
                .satisfies(tables -> assertThat(tables)
                        .anyMatch(table -> !table.valid() && !table.errors().isEmpty() && table.format() == TableFormat.ERROR)
                        .anyMatch(table -> table.valid() && table.path().path().equals(directory.appendPath("table1").toString()) && table.format() == TableFormat.ICEBERG)
                        .anyMatch(table -> table.valid() && table.path().path().equals(directory.appendPath("table2").toString()) && table.format() == TableFormat.ICEBERG));
    }

    @Test
    public void testIcebergWithMetadataLocationWithTrailingSlash(@TempDir Path tempDir)
            throws IOException
    {
        Location tableLocation = prepareTestTableWithCorrectLocation(tempDir, "table1");
        Path latestMetadataPath = Path.of("/" + tableLocation.path())
                .resolve("metadata")
                .resolve("00001-71c31f90-ec3e-448e-9132-be0574b16fb2.metadata.json");
        replaceInJsonFiles(latestMetadataPath.getParent(), tableLocation.toString(), tableLocation + "/");

        Processor processor = new Processor(schemaDiscoveryInstances, Util.fileSystem(), tableLocation, OPTIONS, Executors.newCachedThreadPool());
        processor.startRootProcessing();

        assertThat(processor)
                .succeedsWithin(Duration.ofSeconds(1))
                .matches(discovered -> discovered.rootPath().path().equals(tableLocation + "/"))
                .matches(discovered -> discovered.errors().isEmpty())
                .matches(discovered -> discovered.tables().size() == 1)
                .extracting(discoveredSchema -> discoveredSchema.tables().getFirst())
                .satisfies(table -> assertThat(table)
                        .matches(DiscoveredTable::valid)
                        .matches(t -> t.format() == TableFormat.ICEBERG)
                        // iceberg table path should not have trailing slash
                        .matches(t -> t.path().removeTrailingSlash().equals(t.path()) && t.path().path().equals(tableLocation.toString())));
    }

    private Location setupIcebergTableStructure(Path root, Function<Path, String> metadataFileContentProvider)
            throws IOException
    {
        Path icebergTableDirectory = Files.createDirectory(root.resolve("iceberg_table_" + UUID.randomUUID().toString().replaceAll("-", "")));

        String metadataFileContent = metadataFileContentProvider.apply(icebergTableDirectory);
        Path metadataDirectory = Files.createDirectory(icebergTableDirectory.resolve("metadata"));
        Path metadataFile = Files.createFile(metadataDirectory.resolve("00001-71c31f90-ec3e-448e-9132-be0574b16fb2.metadata.json"));
        Files.writeString(metadataFile, metadataFileContent);

        Files.createDirectory(icebergTableDirectory.resolve("data"));
        return Location.of(icebergTableDirectory.toString());
    }

    private void prepareTestInvalidTable(Path targetDirectory)
            throws IOException
    {
        String tableTemplatePath = Resources.getResource("iceberg/table_invalid").getPath();
        Path tableDirectory = targetDirectory.resolve("table_invalid");
        FileUtils.copyDirectory(
                Path.of(tableTemplatePath).toFile(),
                tableDirectory.toFile());
    }

    private Location prepareTestTableWithCorrectLocation(Path targetDirectory, String tableName)
            throws IOException
    {
        String tableTemplatePath = Resources.getResource("iceberg/table_valid").getPath();
        Path tableDirectory = targetDirectory.resolve(tableName);
        FileUtils.copyDirectory(
                Path.of(tableTemplatePath).toFile(),
                tableDirectory.toFile());
        String tableTrinoFileSystemLocation = toLocalDirectory(tableDirectory).toString();
        replaceInJsonFiles(tableDirectory, "\\$\\{TABLE_LOCATION}", tableTrinoFileSystemLocation);

        return toLocalDirectory(tableDirectory);
    }

    private static void replaceInJsonFiles(Path directory, String regex, String newText)
            throws IOException
    {
        for (File jsonFile : FileUtils.listFiles(directory.toFile(), new String[] {"json"}, true)) {
            String content = Files.readString(jsonFile.toPath());
            Files.writeString(jsonFile.toPath(), content.replaceAll(regex, newText));
        }
    }
}
