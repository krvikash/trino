/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.iceberg;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.INVALID_METADATA;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

/**
 * Partial copy of io.trino.plugin.iceberg.IcebergUtil for avoiding a cyclic reference.
 */
public final class IcebergUtil
{
    private IcebergUtil() {}

    public static final String METADATA_FOLDER_NAME = "metadata";
    public static final String METADATA_FILE_EXTENSION = ".metadata.json";
    // Metadata file name examples
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.gz.metadata.json (https://github.com/apache/iceberg/blob/ab398a0d5ff195f763f8c7a4358ac98fa38a8de7/core/src/main/java/org/apache/iceberg/TableMetadataParser.java#L141)
    //  - 00001-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json.gz (https://github.com/apache/iceberg/blob/ab398a0d5ff195f763f8c7a4358ac98fa38a8de7/core/src/main/java/org/apache/iceberg/TableMetadataParser.java#L146)
    private static final Pattern METADATA_FILE_NAME_PATTERN = Pattern.compile("(?<version>\\d+)-(?<uuid>[-a-fA-F0-9]*)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");
    // Hadoop Generated Metadata file name examples
    //  - v0.metadata.json
    //  - v0.gz.metadata.json
    //  - v0.metadata.json.gz
    private static final Pattern HADOOP_GENERATED_METADATA_FILE_NAME_PATTERN = Pattern.compile("v(?<version>\\d+)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");

    public static int parseVersion(String metadataFileName)
            throws TrinoException
    {
        checkArgument(!metadataFileName.contains("/"), "Not a file name: %s", metadataFileName);
        Matcher matcher = METADATA_FILE_NAME_PATTERN.matcher(metadataFileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        matcher = HADOOP_GENERATED_METADATA_FILE_NAME_PATTERN.matcher(metadataFileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        throw new TrinoException(INVALID_METADATA, "Invalid metadata file name: " + metadataFileName);
    }

    public static String getLatestMetadataLocation(TrinoFileSystem fileSystem, String location)
    {
        List<Location> latestMetadataLocations = new ArrayList<>();
        String metadataDirectoryLocation = format("%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME);
        try {
            int latestMetadataVersion = -1;
            FileIterator fileIterator = fileSystem.listFiles(Location.of(metadataDirectoryLocation));
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                Location fileLocation = fileEntry.location();
                String fileName = fileLocation.fileName();
                if (fileName.endsWith(METADATA_FILE_EXTENSION)) {
                    int versionNumber = parseVersion(fileName);
                    if (versionNumber > latestMetadataVersion) {
                        latestMetadataVersion = versionNumber;
                        latestMetadataLocations.clear();
                        latestMetadataLocations.add(fileLocation);
                    }
                    else if (versionNumber == latestMetadataVersion) {
                        latestMetadataLocations.add(fileLocation);
                    }
                }
            }
            if (latestMetadataLocations.isEmpty()) {
                throw new TrinoException(INVALID_METADATA, "No versioned metadata file exists at location: " + metadataDirectoryLocation);
            }
            if (latestMetadataLocations.size() > 1) {
                throw new TrinoException(INVALID_METADATA, format(
                        "More than one latest metadata file found at location: %s, latest metadata files are %s",
                        metadataDirectoryLocation,
                        latestMetadataLocations));
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(IO, "Failed checking table location: " + location, e);
        }
        return getOnlyElement(latestMetadataLocations).toString();
    }

    /**
     * Copy of io.trino.plugin.iceberg.procedure.RegisterTableProcedure#locationEquivalent(java.lang.String, java.lang.String)
     */
    public static boolean locationEquivalent(String a, String b)
    {
        return normalizeS3Uri(a).equals(normalizeS3Uri(b));
    }

    private static String normalizeS3Uri(String tableLocation)
    {
        // Normalize e.g. s3a to s3, so that table can be registered using s3:// location
        // even if internally it uses s3a:// paths.
        String normalizedSchema = tableLocation.replaceFirst("^s3[an]://", "s3://");
        // Remove trailing slashes so that test_dir is equal to test_dir/
        return stripTrailingSlash(normalizedSchema);
    }
}
