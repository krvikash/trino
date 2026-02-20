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
package io.trino.plugin.warp.cloudstorage.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3Location;
import io.trino.filesystem.s3.S3SseCustomerKey;
import io.trino.plugin.warp.cloudstorage.CloudObjectMetadata;
import io.trino.plugin.warp.cloudstorage.CloudStorageService;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static io.trino.plugin.warp.cloudstorage.s3.S3Utils.getAwsServiceException;
import static io.trino.plugin.warp.cloudstorage.s3.S3Utils.handleAwsException;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AWS_KMS;
import static software.amazon.awssdk.utils.BinaryUtils.fromBase64;
import static software.amazon.awssdk.utils.Md5Utils.md5AsBase64;

public class S3CloudStorage
        extends CloudStorageService
{
    private static final Logger logger = Logger.get(S3CloudStorage.class);

    private final S3AsyncClient client;
    private final S3TransferManager transferManager;
    private final S3FileSystemConfig config;

    public S3CloudStorage(S3FileSystemFactory fileSystemFactory, S3AsyncClient client, S3FileSystemConfig config)
    {
        super(fileSystemFactory);
        this.client = requireNonNull(client, "client is null");
        this.transferManager = S3TransferManager.builder().s3Client(client).build();
        this.config = config;
    }

    @Override
    public CloudObjectMetadata uploadFile(Location source, Location target)
            throws IOException
    {
        target.verifyValidFileLocation();

        S3Location s3Location = new S3Location(target);

        UploadFileRequest request = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(s3Location.bucket())
                        .key(s3Location.key())
                .applyMutation(builder -> {
                    switch (config.getSseType()) {
                        case NONE -> { /* ignored */ }
                        case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(config.getSseKmsKeyId());
                        case CUSTOMER -> {
                            S3SseCustomerKey aes256 = new S3SseCustomerKey(config.getSseCustomerKey(), md5AsBase64(fromBase64(config.getSseCustomerKey())), "AES256");
                            builder.sseCustomerAlgorithm(aes256.algorithm())
                                    .sseCustomerKey(aes256.key())
                                    .sseCustomerKeyMD5(aes256.md5());
                        }
                        default -> builder.serverSideEncryption(AES256);
                    }
                }))
                .source(new File(source.toString()))
                .build();

        try {
            return retry(() -> {
                FileUpload upload = transferManager.uploadFile(request);
                PutObjectResponse response = upload.completionFuture().join().response();
                return new CloudObjectMetadata(response.eTag(), null, response.size());
            }, "uploadFile failed [%s] => [%s]".formatted(source, target));
        }
        catch (Exception e) {
            throw handleAwsException(e, "upload failed", s3Location);
        }
    }

    @Override
    public CloudObjectMetadata downloadFile(Location source, Location target)
            throws IOException
    {
        source.verifyValidFileLocation();

        S3Location s3Location = new S3Location(source);

        DownloadFileRequest request = DownloadFileRequest.builder()
                .getObjectRequest(req -> req.bucket(s3Location.bucket()).key(s3Location.key()))
                .destination(new File(target.toString()))
                .build();

        try {
            return retry(() -> {
                FileDownload download = transferManager.downloadFile(request);
                GetObjectResponse response = download.completionFuture().join().response();
                return new CloudObjectMetadata(response.eTag(), response.lastModified(), response.contentLength());
            }, "downloadFile failed [%s] => [%s]".formatted(source, target));
        }
        catch (Exception e) {
            throw handleAwsException(e, "download failed", s3Location);
        }
    }

    @Override
    public CloudObjectMetadata copyFile(Location source, Location destination)
            throws IOException
    {
        source.verifyValidFileLocation();
        destination.verifyValidFileLocation();

        S3Location sourceLocation = new S3Location(source);
        S3Location targetLocation = new S3Location(destination);

        CopyRequest request = CopyRequest.builder()
                .copyObjectRequest(req -> req.sourceBucket(sourceLocation.bucket())
                        .sourceKey(sourceLocation.key())
                        .destinationBucket(targetLocation.bucket())
                        .destinationKey(targetLocation.key())
                        .applyMutation(builder -> {
                            switch (config.getSseType()) {
                                case NONE -> { /* ignored */ }
                                case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(config.getSseKmsKeyId());
                                case CUSTOMER -> {
                                    S3SseCustomerKey aes256 = new S3SseCustomerKey(config.getSseCustomerKey(), md5AsBase64(fromBase64(config.getSseCustomerKey())), "AES256");
                                    builder.sseCustomerAlgorithm(aes256.algorithm())
                                            .sseCustomerKey(aes256.key())
                                            .sseCustomerKeyMD5(aes256.md5());
                                }
                                default -> builder.serverSideEncryption(AES256);
                            }
                        }))
                .build();

        try {
            Copy copy = transferManager.copy(request);
            CopyObjectResult result = copy.completionFuture().join().response().copyObjectResult();
            return new CloudObjectMetadata(result.eTag(), result.lastModified(), null);
        }
        catch (RuntimeException e) {
            throw handleAwsException(e, "copy failed", sourceLocation);
        }
    }

    @Override
    public boolean copyFileReplaceTail(Location source, Location destination, CloudObjectMetadata metadata, long position, byte[] tailBuffer)
            throws IOException
    {
        source.verifyValidFileLocation();
        destination.verifyValidFileLocation();

        validateS3Location(source);
        validateS3Location(destination);

        try (S3AsyncOutput output = new S3AsyncOutput(client, source, destination, metadata, config)) {
            output.writeTail(position, tailBuffer);
            return true;
        }
        catch (IOException e) {
            AwsServiceException exception = getAwsServiceException(e);
            if ((exception != null) && (exception.statusCode() == 412)) {
                logger.debug("copyFileReplaceTail [%s] => [%s] metadata %s position %d exception: %s",
                        source, destination, metadata, position, e);
                return false;
            }
            throw new IOException("copyFileReplaceTail failed exception: %s cause: %s".formatted(e, e.getCause()), e);
        }
    }

    @Override
    public CloudObjectMetadata renameFile(Location source, Location target)
            throws IOException
    {
        CloudObjectMetadata metadata = copyFile(source, target);
        deleteFile(source);
        return metadata;
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
    {
        try {
            listFiles(location);
            return Optional.of(true);
        }
        catch (IOException e) {
            return Optional.of(false);
        }
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateS3Location(Location location)
    {
        new S3Location(location);
    }

    private <T> T retry(CheckedSupplier<T> supplier, String message)
    {
        return Failsafe.with(RetryPolicy.builder()
                        .withMaxRetries(3)
                        .handleIf(throwable -> {
                            AwsServiceException exception = getAwsServiceException(throwable);
                            return ((exception != null) && (exception.statusCode() == 412));
                        })
                        .onRetry(event -> logger.debug("onRetry %s %s", message, event))
                        .build())
                .get(supplier);
    }
}
