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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.TestingTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.util.Objects.requireNonNull;

public final class S3HiveQueryRunner
{
    static {
        Logging.initialize();
    }

    private S3HiveQueryRunner() {}

    public static QueryRunner create(
            Hive3MinioDataLake hiveMinioDataLake,
            Map<String, String> additionalHiveProperties)
            throws Exception
    {
        return builder(hiveMinioDataLake)
                .setHiveProperties(additionalHiveProperties)
                .build();
    }

    public static Builder builder(HiveMinioDataLake hiveMinioDataLake)
    {
        return builder()
                .setHiveMetastoreEndpoint(hiveMinioDataLake.getHiveMetastoreEndpoint())
                .setS3Endpoint("http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                .setS3Region(MINIO_REGION)
                .setS3AccessKey(MINIO_ROOT_USER)
                .setS3SecretKey(MINIO_ROOT_PASSWORD)
                .setBucketName(hiveMinioDataLake.getBucketName());
    }

    public static Builder builderWithAwsCustomCredentialProvider(HiveMinioDataLake hiveMinioDataLake, String customAwsCredentialProvider)
    {
        return builder()
                .setHiveMetastoreEndpoint(hiveMinioDataLake.getHiveMetastoreEndpoint())
                .setS3Endpoint("http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                .setS3Region(MINIO_REGION)
                .setCustomAwsCredentialProvider(customAwsCredentialProvider)
                .setBucketName(hiveMinioDataLake.getBucketName());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends HiveQueryRunner.Builder<Builder>
    {
        private URI hiveMetastoreEndpoint;
        private Duration thriftMetastoreTimeout = TestingTokenAwareMetastoreClientFactory.TIMEOUT;
        private ThriftMetastoreConfig thriftMetastoreConfig = new ThriftMetastoreConfig();
        private String s3Region;
        private String s3Endpoint;
        private Optional<String> s3AccessKey = Optional.empty();
        private Optional<String> s3SecretKey = Optional.empty();
        private Optional<String> customAwsCredentialProviderClassName = Optional.empty();
        private String bucketName;

        @CanIgnoreReturnValue
        public Builder setHiveMetastoreEndpoint(URI hiveMetastoreEndpoint)
        {
            this.hiveMetastoreEndpoint = requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setThriftMetastoreTimeout(Duration thriftMetastoreTimeout)
        {
            this.thriftMetastoreTimeout = requireNonNull(thriftMetastoreTimeout, "thriftMetastoreTimeout is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setThriftMetastoreConfig(ThriftMetastoreConfig thriftMetastoreConfig)
        {
            this.thriftMetastoreConfig = requireNonNull(thriftMetastoreConfig, "thriftMetastoreConfig is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3Region(String s3Region)
        {
            this.s3Region = requireNonNull(s3Region, "s3Region is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3Endpoint(String s3Endpoint)
        {
            this.s3Endpoint = requireNonNull(s3Endpoint, "s3Endpoint is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3AccessKey(String s3AccessKey)
        {
            this.s3AccessKey = Optional.of(requireNonNull(s3AccessKey, "s3AccessKey is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setS3SecretKey(String s3SecretKey)
        {
            this.s3SecretKey = Optional.of(requireNonNull(s3SecretKey, "s3SecretKey is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setCustomAwsCredentialProvider(String customAwsCredentialProviderClassName)
        {
            this.customAwsCredentialProviderClassName = Optional.of(requireNonNull(customAwsCredentialProviderClassName, "customAwsCredentialProviderClassName is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setBucketName(String bucketName)
        {
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            requireNonNull(s3Region, "s3Region is null");
            requireNonNull(s3Endpoint, "s3Endpoint is null");
            requireNonNull(s3AccessKey, "s3AccessKey is null");
            requireNonNull(s3SecretKey, "s3SecretKey is null");
            requireNonNull(bucketName, "bucketName is null");
            String lowerCaseS3Endpoint = s3Endpoint.toLowerCase(Locale.ENGLISH);
            checkArgument(lowerCaseS3Endpoint.startsWith("http://") || lowerCaseS3Endpoint.startsWith("https://"), "Expected http URI for S3 endpoint; got %s", s3Endpoint);

            addHiveProperty("fs.s3.enabled", "true");
            addHiveProperty("s3.region", s3Region);
            addHiveProperty("s3.endpoint", s3Endpoint);
            s3AccessKey.ifPresent(accessKey -> addHiveProperty("s3.aws-access-key", accessKey));
            s3SecretKey.ifPresent(secretKey -> addHiveProperty("s3.aws-secret-key", secretKey));
            customAwsCredentialProviderClassName.ifPresent(className -> addHiveProperty("s3.custom-credential-provider-class", className));
            addHiveProperty("s3.path-style-access", "true");
            setMetastore(distributedQueryRunner -> new BridgingHiveMetastore(
                    testingThriftHiveMetastoreBuilder()
                            .thriftMetastoreConfig(thriftMetastoreConfig)
                            .metastoreClient(hiveMetastoreEndpoint, thriftMetastoreTimeout)
                            .build(distributedQueryRunner::registerResource)));
            setInitialSchemasLocationBase("s3a://" + bucketName); // cannot use s3:// as Hive metastore is not configured to accept it
            return super.build();
        }
    }

    static void main()
            throws Exception
    {
        Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake("tpch");
        hiveMinioDataLake.start();

        QueryRunner queryRunner = S3HiveQueryRunner.builder(hiveMinioDataLake)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                .setSkipTimezoneSetup(true)
                .setInitialTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(S3HiveQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static class S3Hive4QueryRunner
    {
        static void main()
                throws Exception
        {
            Hive4MinioDataLake hiveMinioDataLake = new Hive4MinioDataLake("tpch");
            hiveMinioDataLake.start();

            QueryRunner queryRunner = S3HiveQueryRunner.builder(hiveMinioDataLake)
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                    .setSkipTimezoneSetup(true)
                    .setInitialTables(TpchTable.getTables())
                    .build();
            Logger log = Logger.get(S3Hive4QueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
