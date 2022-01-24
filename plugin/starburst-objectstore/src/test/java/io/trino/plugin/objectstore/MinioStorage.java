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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";
    public static final String REGION = "us-east-1";

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final String bucketName;
    private final Minio minio;
    private S3Client s3;

    public MinioStorage(String bucketName)
    {
        this(bucketName, Network.newNetwork());
    }

    public MinioStorage(String bucketName, Network network)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.minio = closer.register(Minio.builder()
                .withNetwork(network)
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .buildOrThrow())
                .build());
    }

    public void start()
    {
        minio.start();

        s3 = S3Client.builder()
                .forcePathStyle(true)
                .endpointOverride(URI.create(getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(US_EAST_1)
                .build();
        closer.register(s3);

        s3.createBucket(CreateBucketRequest.builder()
                .bucket(bucketName)
                .build());
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public List<String> listObjects(String key)
    {
        return s3.listObjectsV2Paginator(
                        ListObjectsV2Request.builder()
                                .bucket(bucketName)
                                .prefix(key)
                                .build()).stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key)
                .collect(toImmutableList());
    }

    public void deleteObjects(List<String> keys)
    {
        s3.deleteObjects(
                DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(builder -> builder.objects(keys.stream()
                                .map(key -> ObjectIdentifier.builder().key(key).build())
                                .toList()).quiet(true))
                        .overrideConfiguration(disableStrongIntegrityChecksums())
                        .build());
    }

    // TODO (https://github.com/trinodb/trino/issues/24955):
    // remove me once all of the S3-compatible storage support strong integrity checks
    @SuppressWarnings("deprecation")
    private static AwsRequestOverrideConfiguration disableStrongIntegrityChecksums()
    {
        return AwsRequestOverrideConfiguration.builder()
                .signer(AwsS3V4Signer.create())
                .build();
    }

    public void putObject(String key, String content)
    {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build(),
                RequestBody.fromBytes(content.getBytes(StandardCharsets.UTF_8).clone()));
    }

    @SuppressWarnings("HttpUrlsUsage")
    public String getEndpoint()
    {
        return "http://" + minio.getMinioApiEndpoint();
    }

    public String getS3Url()
    {
        return "s3://" + bucketName;
    }

    public Map<String, String> getNativeS3Config()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", ACCESS_KEY)
                .put("s3.aws-secret-key", SECRET_KEY)
                .put("s3.region", REGION)
                .put("s3.endpoint", getEndpoint())
                .put("s3.path-style-access", "true")
                .buildOrThrow();
    }
}
