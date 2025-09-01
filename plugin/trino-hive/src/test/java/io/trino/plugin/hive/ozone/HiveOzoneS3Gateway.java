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
package io.trino.plugin.hive.ozone;

import io.trino.plugin.hive.containers.HiveHadoop;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_ACCESS_KEY;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public class HiveOzoneS3Gateway
        implements AutoCloseable
{
    private final String bucketName;
    private final S3Client s3Client;
    private final HiveOzoneDataLake hiveOzoneDataLake;

    public HiveOzoneS3Gateway(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.hiveOzoneDataLake = new HiveOzoneDataLake();

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(hiveOzoneDataLake.getApacheOzoneContainer().getS3EndpointAddress()))
                // If security is not enabled, we can use any AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(DUMMY_ACCESS_KEY, DUMMY_SECRET_KEY)))
                .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @Override
    public void close()
            throws Exception
    {
        hiveOzoneDataLake.close();
    }

    public HiveHadoop getHiveHadoop()
    {
        return hiveOzoneDataLake.getHiveHadoop();
    }

    public ApacheOzoneContainer getApacheOzoneContainer()
    {
        return hiveOzoneDataLake.getApacheOzoneContainer();
    }

    public List<String> listFiles(String targetDirectory)
    {
        String prefix = "s3://" + bucketName + "/";
        String key = targetDirectory;
        if (targetDirectory.startsWith(prefix)) {
            key = targetDirectory.substring(prefix.length());
        }

        return s3Client.listObjects(ListObjectsRequest.builder()
                        .bucket(bucketName)
                        .prefix(key)
                        .build())
                .contents().stream()
                .map(S3Object::key)
                .collect(toImmutableList());
    }

    public void deleteFile(String targetDirectory)
    {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(targetDirectory)
                .build());
    }
}
