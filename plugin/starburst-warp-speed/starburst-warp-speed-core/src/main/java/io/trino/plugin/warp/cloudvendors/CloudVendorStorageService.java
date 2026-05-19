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
package io.trino.plugin.warp.cloudvendors;

import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.warp.cloudstorage.CloudObjectMetadata;
import io.trino.plugin.warp.cloudstorage.CloudStorage;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.tools.ByteBufferInputStream;
import io.trino.plugin.warp.tools.util.CompressionUtil;
import io.trino.plugin.warp.tools.util.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public class CloudVendorStorageService
        extends CloudVendorService
{
    private static final Logger logger = Logger.get(CloudVendorStorageService.class);
    private final ShapingLogger shapingLogger;

    private final CloudStorage cloudStorage;

    public CloudVendorStorageService(CloudStorage cloudStorage, ShapingLoggerFactory shapingLoggerFactory)
    {
        this.cloudStorage = requireNonNull(cloudStorage);
        shapingLogger = shapingLoggerFactory.getInstance(CloudVendorStorageService.class);
    }

    @Override
    public void uploadToCloud(byte[] bytes, String outputPath)
    {
        Location location = getLocation(outputPath);
        TrinoOutputFile outputFile = cloudStorage.newOutputFile(location);

        String message = String.format(Locale.US, "uploadToCloud bytes length %d => location [%s]", bytes.length, location);
        logger.debug(message);

        try (OutputStream outputStream = outputFile.create()) {
            outputStream.write(bytes);
        }
        catch (IOException e) {
            throw new RuntimeException("%s exception: %s cause: %s".formatted(message, e, e.getCause()), e);
        }
    }

    @Override
    public void uploadFileToCloud(String localInputPath, String outputPath)
    {
        if (StringUtils.isEmpty(localInputPath) || StringUtils.isEmpty(outputPath)) {
            throw new IllegalArgumentException("in/out path is null/empty");
        }
        if (!new File(localInputPath).exists()) {
            throw new IllegalArgumentException("input path file doesnt exist");
        }
        try {
            logger.debug("uploadFileToCloud [%s] => [%s]", getLocation(localInputPath), getLocation(outputPath));
            cloudStorage.uploadFile(getLocation(localInputPath), getLocation(outputPath));
        }
        catch (Exception e) {
            throw new RuntimeException("uploadFileToCloud failed [%s] => [%s] exception: %s cause: %s"
                    .formatted(localInputPath, outputPath, e, e.getCause()), e);
        }
    }

    @Override
    public CloudVendorResult uploadFileToCloud(String outputPath, File localFile, Callable<Boolean> validateBeforeDo)
    {
        Location destination = getLocation(outputPath + getTempFileSuffix());

        try {
            logger.debug("uploadFileToCloud [%s] => [%s]", getLocation(localFile.getPath()), destination);
            cloudStorage.uploadFile(getLocation(localFile.getPath()), destination);
            return validateAndRename(validateBeforeDo, destination, getLocation(outputPath), localFile.length());
        }
        catch (Exception e) {
            throw new RuntimeException("uploadFileToCloud failed [%s] => [%s] exception: %s cause: %s"
                    .formatted(localFile.getPath(), outputPath, e, e.getCause()), e);
        }
    }

    @Override
    public Optional<String> downloadCompressedFromCloud(String cloudPath, boolean allowKeyNotFound)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        String message = String.format(Locale.US, "downloadCompressedFromCloud location [%s]", location);

        try (TrinoInput input = inputFile.newInput()) {
            int length = (int) inputFile.length();
            byte[] bytes = new byte[length];

            logger.debug("%s => bytes length %d", message, bytes.length);

            input.readFully(0, bytes, 0, length);
            String json = CompressionUtil.decompressGzip(bytes);

            return Optional.of(json);
        }
        catch (IOException e) {
            if (allowKeyNotFound) {
                return Optional.empty();
            }
            throw new RuntimeException("%s exception: %s cause: %s".formatted(message, e, e.getCause()), e);
        }
    }

    @Override
    public InputStream downloadRangeFromCloud(String cloudPath, long startOffset, int length)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        String message = String.format(
                Locale.US,
                "downloadRangeFromCloud location [%s] startOffset %d length %d",
                location,
                startOffset,
                length);
        logger.debug(message);

        try (TrinoInput input = inputFile.newInput()) {
            byte[] bytes = new byte[length];

            input.readFully(startOffset, bytes, 0, length);
            return new ByteBufferInputStream(ByteBuffer.wrap(bytes), length);
        }
        catch (IOException e) {
            throw new RuntimeException("%s exception: %s cause: %s".formatted(message, e, e.getCause()), e);
        }
    }

    @Override
    public StorageObjectMetadata downloadFileFromCloud(String cloudPath, File localFile)
    {
        try {
            logger.debug("downloadFileFromCloud [%s] => [%s]", getLocation(cloudPath), getLocation(localFile.getPath()));
            CloudObjectMetadata metadata = cloudStorage.downloadFile(getLocation(cloudPath), getLocation(localFile.getPath()));
            return new StorageObjectMetadata(metadata);
        }
        catch (Exception e) {
            throw new RuntimeException("downloadFileFromCloud failed [%s] => [%s] exception: %s cause: %s"
                    .formatted(cloudPath, localFile.getPath(), e, e.getCause()), e);
        }
    }

    @Override
    public CloudVendorResult appendOnCloud(
            String cloudPath,
            File localFile,
            StorageObjectMetadata metadata,
            long startOffset,
            boolean isSparseFile,
            Callable<Boolean> validateBeforeDo)
    {
        Location source = getLocation(cloudPath);
        Location destination = getLocation(cloudPath + getTempFileSuffix());

        String message = String.format(
                Locale.US,
                "appendOnCloud source [%s] => destination [%s] metadata %s startOffset %d",
                source,
                destination,
                metadata,
                startOffset);
        logger.debug(message);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(localFile, "rw")) {
            int length = (int) (localFile.length() - startOffset);
            byte[] buffer = new byte[length];

            randomAccessFile.seek(startOffset);
            length = randomAccessFile.read(buffer);

            if (length > 0) {
                if (!cloudStorage.copyFileReplaceTail(source, destination, metadata.getCloudObjectMetadata(), startOffset, buffer)) {
                    return new CloudVendorResult();
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("%s exception: %s cause: %s".formatted(message, e, e.getCause()), e);
        }

        try {
            return validateAndRename(validateBeforeDo, destination, source, localFile.length());
        }
        catch (Exception e) {
            throw new RuntimeException("appendOnCloud failed after copyFileReplaceTail exception: %s cause: %s".formatted(e, e.getCause()), e);
        }
    }

    private CloudVendorResult validateAndRename(
            Callable<Boolean> validateBeforeDo,
            Location destination,
            Location source,
            long contentLength)
            throws Exception
    {
        boolean isUploadDone = false;
        StorageObjectMetadata metadata;

        if ((validateBeforeDo == null) || validateBeforeDo.call()) {
            CloudObjectMetadata objectMetadata = cloudStorage.renameFile(destination, source);

            metadata = new StorageObjectMetadata(objectMetadata);
            if (metadata.getContentLength().isEmpty()) {
                metadata.setContentLength(contentLength);
            }
            isUploadDone = true;
        }
        else {
            cloudStorage.deleteFile(destination);
            metadata = new StorageObjectMetadata();
        }
        return new CloudVendorResult(isUploadDone, metadata);
    }

    @Override
    public List<String> listPath(String cloudPath, boolean isTopLevel)
    {
        Location location = getLocation(cloudPath);

        String message = String.format(Locale.US, "listPath location [%s] isTopLevel %s", location, isTopLevel);
        logger.debug(message);

        try {
            if (isTopLevel) {
                Set<Location> directories = cloudStorage.listDirectories(location);
                return directories.stream().map(Location::toString).toList();
            }
            else {
                FileIterator fileIterator = cloudStorage.listFiles(location);
                List<String> paths = new ArrayList<>();

                while (fileIterator.hasNext()) {
                    FileEntry fileEntry = fileIterator.next();
                    paths.add(fileEntry.location().toString());
                }
                return paths;
            }
        }
        catch (IOException e) {
            throw new RuntimeException("%s exception: %s cause: %s".formatted(message, e, e.getCause()), e);
        }
    }

    @Override
    public boolean directoryExists(String cloudPath)
    {
        // normalize to directory
        Location location = cloudPath.endsWith("/") ? getLocation(cloudPath) : getLocation(cloudPath + "/");
        try {
            logger.debug("directoryExists location [%s]", location);
            return cloudStorage.directoryExists(location).orElse(false);
        }
        catch (IOException e) {
            throw new RuntimeException("directoryExists [%s] failed exception: %s cause: %s".formatted(location, e, e.getCause()), e);
        }
    }

    @Override
    public StorageObjectMetadata getObjectMetadata(String cloudPath)
    {
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        try {
            long length = inputFile.length();
            Instant lastModified = inputFile.lastModified();

            storageObjectMetadata.setContentLength(length);
            storageObjectMetadata.setLastModified(lastModified.toEpochMilli());
            storageObjectMetadata.setETag(StorageObjectMetadata.ETAG_UNKNOWN);
            logger.debug("getObjectMetadata location [%s] length %d lastModified %s", location, length, lastModified);
        }
        catch (IOException e) {
            if ((e instanceof FileNotFoundException) || (e.getCause() instanceof FileNotFoundException)) {
                logger.debug(e, "getObjectMetadata failed location [%s]".formatted(location));
            }
            else {
                shapingLogger.error(e, "getObjectMetadata failed location [%s]", location);
            }
            // do nothing
//            throw new RuntimeException(e);
        }
        return storageObjectMetadata;
    }

    @Override
    public Optional<Long> getLastModified(String cloudPath)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        try {
            Instant lastModified = inputFile.lastModified();
            logger.debug("getLastModified location [%s] lastModified %s", location, lastModified);
            return Optional.of(lastModified.toEpochMilli());
        }
        catch (IOException e) {
            if ((e instanceof FileNotFoundException) || (e.getCause() instanceof FileNotFoundException)) {
                logger.debug(e, "getLastModified failed location [%s]".formatted(location));
            }
            else {
                shapingLogger.error(e, "getLastModified failed location [%s]", location);
            }
            // do nothing
//            throw new RuntimeException(e);
        }
        return Optional.empty();
    }
}
