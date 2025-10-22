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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.spi.connector.ConnectorContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.endpoint.AwsClientEndpointProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class S3CloudStorageModule
        implements Module
{
    private static final Logger logger = Logger.get(S3CloudStorageModule.class);

    private final ConnectorContext context;
    private final ConfigurationFactory configFactory;
    private final Class<? extends Annotation> annotation;

    public S3CloudStorageModule(ConnectorContext context,
                                ConfigurationFactory configFactory,
                                Class<? extends Annotation> annotation)
    {
        this.context = requireNonNull(context, "context is null");
        this.configFactory = requireNonNull(configFactory, "configFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configFactory);

        configBinder(binder).bindConfig(S3FileSystemConfig.class);

        binder.install(new ConnectorContextModule(context));
        binder.bind(S3FileSystemFactory.class);

        binder.bind(S3CloudStorage.class).annotatedWith(annotation).to(S3CloudStorage.class);
    }

    @Provides
    @Singleton
    public S3CloudStorage provideS3CloudStorage(S3FileSystemFactory fileSystemFactory, S3FileSystemConfig config)
    {
        S3FileSystemFactory s3FileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        S3AsyncClient client = createS3AsyncClient(config);
        return new S3CloudStorage(s3FileSystemFactory, client, config);
    }

    private S3AsyncClient createS3AsyncClient(S3FileSystemConfig config)
    {
        S3CrtAsyncClientBuilder s3 = S3AsyncClient.crtBuilder();
        Region region = getRegion(config);

        s3.credentialsProvider(getAwsCredentialsProvider(config, annotation));
        s3.region(region);
        s3.endpointOverride(Optional.ofNullable(config.getEndpoint()).map(URI::create)
                .orElseGet(() -> AwsClientEndpointProvider.builder()
                        .serviceEndpointPrefix("s3")
                        .defaultProtocol("http")
                        .region(region)
                        .build()
                        .clientEndpoint()));
        s3.forcePathStyle(config.isPathStyleAccess());

        S3CrtHttpConfiguration httpConfiguration = S3CrtHttpConfiguration.builder()
                .trustAllCertificatesEnabled(true)
                .build();
        s3.httpConfiguration(httpConfiguration);

        return s3.build();
    }

    private static Optional<StaticCredentialsProvider> getStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }

    private static StsAssumeRoleCredentialsProvider getStsAssumeRoleCredentialsProvider(S3FileSystemConfig config, Class<? extends Annotation> annotation)
    {
        StsClientBuilder sts = StsClient.builder();

        Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(config.getStsRegion())
                .or(() -> Optional.ofNullable(config.getRegion()))
                .map(Region::of).ifPresent(sts::region);

        Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);
        staticCredentialsProvider.ifPresent(sts::credentialsProvider);
        if (staticCredentialsProvider.isPresent()) {
            logger.info("annotation %s using StaticCredentialsProvider for STS client", annotation.toString());
        }
        else {
            logger.info("annotation %s no StaticCredentials provided for STS client, using DefaultCredentialsProvider chain", annotation.toString());
        }

        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(config.getIamRole())
                        .roleSessionName(config.getRoleSessionName())
                        .externalId(config.getExternalId()))
                .stsClient(sts.build())
                .asyncCredentialUpdateEnabled(true)
                .build();
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(S3FileSystemConfig config, Class<? extends Annotation> annotation)
    {
        AwsCredentialsProvider credentialsProvider;

        if (config.getIamRole() != null) {
            logger.info("annotation %s using StsAssumeRoleCredentialsProvider for STS refresh", annotation.toString());
            credentialsProvider = getStsAssumeRoleCredentialsProvider(config, annotation);
        }
        else {
            logger.info("annotation %s no AssumeRoleCredentials provided for STS refresh", annotation.toString());
            Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);

            if (staticCredentialsProvider.isPresent()) {
                logger.info("annotation %s using StaticCredentialsProvider for S3 client", annotation.toString());
                credentialsProvider = staticCredentialsProvider.orElseThrow(() -> new IllegalArgumentException("cannot use static credentials"));
            }
            else {
                logger.info("annotation %s no StaticCredentials provided for S3 client, using DefaultCredentialsProvider chain", annotation.toString());
                credentialsProvider = DefaultCredentialsProvider.builder().build();
            }
        }

        return credentialsProvider;
    }

    private static Region getRegion(S3FileSystemConfig config)
    {
        return (config.getRegion() != null) ? Region.of(config.getRegion()) : DefaultAwsRegionProviderChain.builder().build().getRegion();
    }
}
