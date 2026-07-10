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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.time.Duration;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.regions.Region.AWS_GLOBAL;

public class TestS3FileSystemAwsS3WithMultiRegionAccessPoint
        extends AbstractTestS3FileSystem
{
    private String accessKey;
    private String secretKey;
    private String mrapArn;

    @Override
    protected boolean usesAwsCrtHttpClient()
    {
        return true;
    }

    @Override
    protected void initEnvironment()
    {
        accessKey = requireEnv("AWS_ACCESS_KEY_ID");
        secretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
        mrapArn = requireEnv("AWS_MRAP_ARN");
    }

    @Override
    protected String bucket()
    {
        return mrapArn;
    }

    @Override
    protected S3Client createS3Client()
    {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(AWS_GLOBAL)
                .httpClient(AwsCrtHttpClient.builder()
                        .maxConcurrency(100)
                        .connectionAcquisitionTimeout(Duration.ofSeconds(30))
                        .build())
                .serviceConfiguration(S3Configuration.builder()
                        .useArnRegionEnabled(true)
                        .pathStyleAccessEnabled(false)
                        .build())
                .build();
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        DataSize streamingPartSize = DataSize.valueOf("5.5MB");
        assertThat(streamingPartSize)
                .describedAs("Configured part size should be less than test's larger file size")
                .isLessThan(LARGER_FILE_DATA_SIZE);

        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey)
                        .setRegion(AWS_GLOBAL.toString())
                        .setMultiRegionAccessPointsEnabled(true)
                        .setPathStyleAccess(false)
                        .setStreamingPartSize(streamingPartSize),
                new S3FileSystemStats());
    }
}
