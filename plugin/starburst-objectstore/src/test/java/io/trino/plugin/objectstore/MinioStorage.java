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
import io.trino.testing.containers.Floci;
import org.testcontainers.containers.Network;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = FLOCI_ACCESS_KEY;
    public static final String SECRET_KEY = FLOCI_SECRET_KEY;
    public static final String REGION = FLOCI_REGION;

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final String bucketName;
    private final Floci floci;
    private S3Client s3;

    public MinioStorage(String bucketName)
    {
        this(bucketName, Network.newNetwork());
    }

    public MinioStorage(String bucketName, Network network)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        // The "floci" alias matches the fs.s3a.endpoint in hive_floci_datalake/hive-core-site.xml
        // mounted into the Hadoop container by HiveMinioStorage
        this.floci = closer.register(new Floci()
                .withNetwork(network)
                .withNetworkAliases("floci"));
    }

    public void start()
    {
        floci.start();

        s3 = floci.createS3Client();
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

    public String getEndpoint()
    {
        return floci.endpoint().toString();
    }

    public String getS3Url()
    {
        return "s3://" + bucketName;
    }

    public Map<String, String> getNativeS3Config()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.s3.enabled", "true")
                .put("s3.aws-access-key", ACCESS_KEY)
                .put("s3.aws-secret-key", SECRET_KEY)
                .put("s3.region", REGION)
                .put("s3.endpoint", getEndpoint())
                .put("s3.path-style-access", "true")
                .buildOrThrow();
    }
}
