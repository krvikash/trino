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
package io.trino.filesystem.s3;

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;

import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.core.checksums.ResponseChecksumValidation.WHEN_REQUIRED;

public class TestS3FileSystemMinIoWithCustomCredentialProvider
        extends AbstractTestS3FileSystem
{
    private final String bucket = "test-bucket-test-s3-custom-credential-provider";

    private Minio minio;

    @Override
    protected void initEnvironment()
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(bucket);
    }

    @AfterAll
    void tearDown()
    {
        if (minio != null) {
            minio.close();
            minio = null;
        }
    }

    @Override
    protected String bucket()
    {
        return bucket;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(MINIO_REGION))
                .forcePathStyle(true)
                .responseChecksumValidation(WHEN_REQUIRED)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setRegion(MINIO_REGION)
                .setPathStyleAccess(true)
                .setCustomCredentialProviderClass(CustomCredentialProviders.MapBasedAwsCredentialsProvider.class.getName())
                .setCustomCredentialProviderArguments("accessKey=%s,secretKey=%s".formatted(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD))
                .setStreamingPartSize(DataSize.valueOf("5.5MB")), new S3FileSystemStats());
    }

    @Test
    @Override
    public void testPaths()
    {
        assertThatThrownBy(super::testPaths)
                .isInstanceOf(IOException.class)
                // MinIO does not support object keys with directory navigation ("/./" or "/../") or with double slashes ("//")
                .hasMessage("S3 HEAD request failed for file: s3://" + bucket + "/test/.././/file");
    }

    @Test
    @Override
    public void testListFiles()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFiles(true);
    }

    @Test
    @Override
    public void testListFilesStartingFrom()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFilesStartingFrom(true);
    }

    @Test
    @Override
    public void testDeleteDirectory()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testDeleteDirectory(true);
    }

    @Test
    @Override
    public void testListDirectories()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListDirectories(true);
    }

    @Test
    public void testWithMissingCustomCredentialProvider()
    {
        assertThatThrownBy(() -> new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(minio.getMinioAddress())
                        .setRegion(MINIO_REGION)
                        .setPathStyleAccess(true)
                        .setCustomCredentialProviderClass("MissingCustomCredentialProvider"),
                new S3FileSystemStats()))
                .hasMessageContaining("AwsCredentialsProvider MissingCustomCredentialProvider not found");
    }

    @Test
    public void testWithInvalidCustomCredentialProviderConstructor()
    {
        assertThatThrownBy(() -> new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(minio.getMinioAddress())
                        .setRegion(MINIO_REGION)
                        .setPathStyleAccess(true)
                        .setCustomCredentialProviderClass(CustomCredentialProviders.AwsCredentialsProviderWithMissingConstructor.class.getName()),
                new S3FileSystemStats()))
                .hasMessageContaining("Unable to initialize AwsCredentialsProvider %s".formatted(CustomCredentialProviders.AwsCredentialsProviderWithMissingConstructor.class.getName()));
    }
}
