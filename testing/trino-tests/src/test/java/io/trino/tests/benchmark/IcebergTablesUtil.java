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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FILE_EXTENSION;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
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
        try (S3Client s3Client = createS3Client(minio)) {
            ensureBucketExists(s3Client, bucketName);

            for (String table : tables) {
                Path tableDir = findTableDirectory(tablesDir, table);
                String tableDirName = tableDir.getFileName().toString();
                String s3TablePath = s3Prefix + "/" + tableDirName;
                String tableLocation = "s3://%s/%s".formatted(bucketName, s3TablePath);

                if (isUploadComplete(tableDir, s3Client, bucketName, s3TablePath)) {
                    log.info("Reusing existing Minio data from s3://%s/%s", bucketName, s3TablePath);
                }
                else {
                    uploadToBucket(tableDir, bucketName, s3TablePath, s3Client);
                }
                Optional<String> metadataPath = getMetadataPath(s3Client, bucketName, s3TablePath);

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

    private static void uploadToBucket(Path tableDir, String bucketName, String s3TablePath, S3Client s3Client)
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
                        s3Client.putObject(builder -> builder.bucket(bucketName).key(s3Path), RequestBody.fromBytes(readAllBytesUnchecked(file)));
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to walk table directory: " + tableDir, e);
        }
    }

    private static boolean isUploadComplete(Path tableDir, S3Client s3Client, String bucketName, String s3TablePath)
    {
        Set<String> localFiles = listLocalFiles(tableDir);
        Set<String> remoteFiles = listObjects(s3Client, bucketName, s3TablePath + "/").stream()
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

    private static Optional<String> getMetadataPath(S3Client s3Client, String bucketName, String s3TablePath)
    {
        return listObjects(s3Client, bucketName, s3TablePath + "/metadata/").stream()
                .filter(path -> path.endsWith(METADATA_FILE_EXTENSION))
                .max(Comparator.naturalOrder());
    }

    private static S3Client createS3Client(Minio minio)
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .build();
    }

    private static void ensureBucketExists(S3Client s3Client, String bucketName)
    {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucketName));
        }
        catch (NoSuchBucketException e) {
            s3Client.createBucket(builder -> builder.bucket(bucketName));
        }
    }

    private static List<String> listObjects(S3Client s3Client, String bucketName, String prefix)
    {
        return s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName).prefix(prefix)).contents().stream()
                .map(S3Object::key)
                .collect(toImmutableList());
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
