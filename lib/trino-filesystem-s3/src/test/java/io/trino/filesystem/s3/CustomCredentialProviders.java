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

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public final class CustomCredentialProviders
{
    private CustomCredentialProviders() {}

    public static class MinIoAwsCredentialsProvider
            implements AwsCredentialsProvider
    {
        public MinIoAwsCredentialsProvider(Map<String, String> properties) {}

        @Override
        public AwsCredentials resolveCredentials()
        {
            return AwsBasicCredentials.create(MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
        }
    }

    public static class MapBasedAwsCredentialsProvider
            implements AwsCredentialsProvider
    {
        private final Map<String, String> credentials;

        public MapBasedAwsCredentialsProvider(Map<String, String> credentials)
        {
            this.credentials = ImmutableMap.copyOf(credentials);
        }

        @Override
        public AwsCredentials resolveCredentials()
        {
            return AwsBasicCredentials.create(credentials.get("accessKey"), credentials.get("secretKey"));
        }
    }

    public static class AwsCredentialsProviderWithMissingConstructor
            implements AwsCredentialsProvider
    {
        @Override
        public AwsCredentials resolveCredentials()
        {
            return AwsBasicCredentials.create("accessKey", "secretKey");
        }
    }
}
