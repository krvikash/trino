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
package io.trino.tests.benchmark;

import io.airlift.log.Logger;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FILE_EXTENSION;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

final class IcebergTablesUtil
{
    private IcebergTablesUtil() {}

    private static final Logger log = Logger.get(IcebergTablesUtil.class);

    public static Path findTableDirectory(Path dataLocation, String table)
    {
        try (Stream<Path> dirs = Files.list(dataLocation)) {
            List<Path> matches = dirs
                    .filter(Files::isDirectory)
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.equals(table) || name.startsWith(table + "-");
                    })
                    .toList();
            if (matches.isEmpty()) {
                throw new IllegalStateException("No directory found for table '%s' in %s".formatted(table, dataLocation));
            }
            if (matches.size() > 1) {
                throw new IllegalStateException("Multiple directories found for table '%s' in %s: %s".formatted(table, dataLocation, matches));
            }
            return matches.getFirst();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path resolveTablesLocation(String dataLocation)
    {
        return Path.of(dataLocation, "tables");
    }

    // Uploads table data from the local filesystem into Minio, duplicating it on disk. The upload is
    // skipped on subsequent runs if the data already exists in the bucket.
    public static void registerTables(Minio minio, HiveMetastore hiveMetastore, String bucketName, String dataLocation, String schemaName, List<String> tables, String s3Prefix)
    {
        Path tablesDir = resolveTablesLocation(dataLocation);
        try (MinioClient minioClient = minio.createMinioClient()) {
            minioClient.ensureBucketExists(bucketName);

            for (String table : tables) {
                Path tableDir = findTableDirectory(tablesDir, table);
                String tableDirName = tableDir.getFileName().toString();
                String s3TablePath = s3Prefix + "/" + tableDirName;
                String tableLocation = "s3://%s/%s".formatted(bucketName, s3TablePath);

                if (isUploadComplete(tableDir, minioClient, bucketName, s3TablePath)) {
                    log.info("Reusing existing Minio data from s3://%s/%s", bucketName, s3TablePath);
                }
                else {
                    uploadToBucket(tableDir, bucketName, s3TablePath, minioClient);
                }
                Optional<String> metadataPath = getMetadataPath(minioClient, bucketName, s3TablePath);

                if (hiveMetastore.getTable(schemaName, table).isPresent()) {
                    log.info("Reusing existing iceberg.%s.%s", schemaName, table);
                    continue;
                }

                String metadataLocation = "s3://%s/%s".formatted(
                        bucketName,
                        metadataPath.orElseThrow(() -> new IllegalStateException("No metadata file found for table %s".formatted(table))));

                log.info("Registering table %s using metadata location %s", table, metadataLocation);
                hiveMetastore.createTable(
                        Table.builder()
                                .setDatabaseName(schemaName)
                                .setTableName(table)
                                .setOwner(Optional.empty())
                                .setTableType(EXTERNAL_TABLE.name())
                                .setDataColumns(List.of())
                                .withStorage(storage -> storage.setLocation(tableLocation))
                                .withStorage(storage -> storage.setStorageFormat(ICEBERG_METASTORE_STORAGE_FORMAT))
                                .setParameter("EXTERNAL", "TRUE")
                                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                                .setParameter(METADATA_LOCATION_PROP, metadataLocation)
                                .build(),
                        NO_PRIVILEGES);
            }
        }
    }

    private static void uploadToBucket(Path tableDir, String bucketName, String s3TablePath, MinioClient minioClient)
    {
        log.info("Uploading %s to s3://%s/%s", tableDir, bucketName, s3TablePath);
        try (Stream<Path> files = Files.walk(tableDir)) {
            // Upload metadata/ files last so that the reuse check (which verifies metadata
            // file presence) cannot pass until all data files are already uploaded.
            files.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing((Path file) -> isMetadataFile(tableDir, file)))
                    .forEach(file -> {
                        String relativePath = tableDir.relativize(file).toString();
                        String s3Path = s3TablePath + "/" + relativePath;
                        minioClient.putObject(bucketName, readAllBytesUnchecked(file), s3Path);
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to walk table directory: " + tableDir, e);
        }
    }

    private static boolean isUploadComplete(Path tableDir, MinioClient minioClient, String bucketName, String s3TablePath)
    {
        Set<String> localFiles = listLocalFiles(tableDir);
        Set<String> remoteFiles = minioClient.listObjects(bucketName, s3TablePath + "/").stream()
                .map(key -> key.substring(s3TablePath.length() + 1))
                .collect(toImmutableSet());
        return localFiles.equals(remoteFiles);
    }

    private static Set<String> listLocalFiles(Path tableDir)
    {
        try (Stream<Path> files = Files.walk(tableDir)) {
            return files.filter(Files::isRegularFile)
                    .map(file -> tableDir.relativize(file).toString())
                    .collect(toImmutableSet());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isMetadataFile(Path tableDir, Path file)
    {
        return tableDir.relativize(file).startsWith(Path.of("metadata"));
    }

    private static Optional<String> getMetadataPath(MinioClient minioClient, String bucketName, String s3TablePath)
    {
        return minioClient.listObjects(bucketName, s3TablePath + "/metadata/").stream()
                .filter(path -> path.endsWith(METADATA_FILE_EXTENSION))
                .max(Comparator.naturalOrder());
    }

    private static byte[] readAllBytesUnchecked(Path path)
    {
        try {
            return Files.readAllBytes(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
