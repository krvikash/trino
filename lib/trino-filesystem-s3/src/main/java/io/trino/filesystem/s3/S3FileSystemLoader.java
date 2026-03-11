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

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3Context.S3SseContext;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.getRetryStrategy;
import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;
import static io.trino.filesystem.s3.S3FileSystemUtils.createStaticCredentialsProvider;
import static io.trino.filesystem.s3.S3FileSystemUtils.createStsClient;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static software.amazon.awssdk.core.checksums.ResponseChecksumValidation.WHEN_REQUIRED;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.SIGNER;

public final class S3FileSystemLoader
        implements Function<Location, TrinoFileSystemFactory>
{
    private final Optional<S3SecurityMappingProvider> mappingProvider;
    private final SdkHttpClient httpClient;
    private final S3ClientFactory clientFactory;
    private final S3FileSystemConfig config;
    private final S3Context context;
    private final ExecutorService uploadExecutor = newCachedThreadPool(daemonThreadsNamed("s3-upload-%s"));
    private final Function<Optional<S3SecurityMappingResult>, S3Client> s3ClientProvider;
    private final Map<Optional<S3SecurityMappingResult>, S3Presigner> preSigners = new ConcurrentHashMap<>();

    @Inject
    public S3FileSystemLoader(S3SecurityMappingProvider mappingProvider, OpenTelemetry openTelemetry, S3FileSystemConfig config, @ForS3ClientCaching boolean s3ClientCaching, S3FileSystemStats stats)
    {
        this(Optional.of(mappingProvider), openTelemetry, config, s3ClientCaching, stats);
    }

    S3FileSystemLoader(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this(Optional.empty(), openTelemetry, config, true, stats);
    }

    private S3FileSystemLoader(Optional<S3SecurityMappingProvider> mappingProvider, OpenTelemetry openTelemetry, S3FileSystemConfig config, boolean s3ClientCaching, S3FileSystemStats stats)
    {
        this.mappingProvider = requireNonNull(mappingProvider, "mappingProvider is null");
        this.httpClient = createHttpClient(config);

        requireNonNull(stats, "stats is null");

        MetricPublisher metricPublisher = stats.newMetricPublisher();
        this.clientFactory = s3ClientFactory(httpClient, openTelemetry, config, metricPublisher);
        this.config = requireNonNull(config, "config is null");
        this.context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                S3SseContext.of(
                        config.getSseType(),
                        config.getSseKmsKeyId(),
                        config.getSseCustomerKey()),
                Optional.empty(),
                config.getStorageClass(),
                config.getCannedAcl());
        if (s3ClientCaching) {
            Map<Optional<S3SecurityMappingResult>, S3Client> clients = new ConcurrentHashMap<>();
            this.s3ClientProvider = mapping -> clients.computeIfAbsent(mapping, clientFactory::create);
        }
        else {
            this.s3ClientProvider = clientFactory::create;
        }
    }

    @Override
    public TrinoFileSystemFactory apply(Location location)
    {
        return identity -> {
            Optional<S3SecurityMappingResult> mapping = mappingProvider.orElseThrow().getMapping(identity, location);

            S3Client client = s3ClientProvider.apply(mapping);
            S3Presigner preSigner = preSigners.computeIfAbsent(mapping, _ -> createS3PreSigner(config, client));
            S3Context context = this.context.withCredentials(identity);

            if (mapping.isPresent() && mapping.get().kmsKeyId().isPresent()) {
                checkState(mapping.get().sseCustomerKey().isEmpty(), "Both SSE-C and KMS-managed keys cannot be used at the same time");
                context = context.withKmsKeyId(mapping.get().kmsKeyId().get());
            }

            if (mapping.isPresent() && mapping.get().sseCustomerKey().isPresent()) {
                context = context.withSseCustomerKey(mapping.get().sseCustomerKey().get());
            }

            return new S3FileSystem(uploadExecutor, client, preSigner, context);
        };
    }

    @PreDestroy
    public void destroy()
    {
        try (httpClient) {
            uploadExecutor.shutdownNow();
        }
    }

    S3Client createClient()
    {
        return clientFactory.create(Optional.empty());
    }

    S3Context context()
    {
        return context;
    }

    Executor uploadExecutor()
    {
        return uploadExecutor;
    }

    private static S3ClientFactory s3ClientFactory(SdkHttpClient httpClient, OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        ClientOverrideConfiguration overrideConfiguration = createOverrideConfiguration(openTelemetry, config, metricPublisher);

        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(config);
        Optional<String> staticRegion = Optional.ofNullable(config.getRegion());
        Optional<String> staticEndpoint = Optional.ofNullable(config.getEndpoint());
        boolean pathStyleAccess = config.isPathStyleAccess();
        boolean useWebIdentityTokenCredentialsProvider = config.isUseWebIdentityTokenCredentialsProvider();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();
        boolean multiRegionAccessPointsEnabled = config.isMultiRegionAccessPointsEnabled();

        return mapping -> {
            Optional<AwsCredentialsProvider> credentialsProvider = mapping
                    .flatMap(S3SecurityMappingResult::credentialsProvider)
                    .or(() -> staticCredentialsProvider);

            Optional<String> region = mapping.flatMap(S3SecurityMappingResult::region).or(() -> staticRegion);
            Optional<String> endpoint = mapping.flatMap(S3SecurityMappingResult::endpoint).or(() -> staticEndpoint);

            Optional<String> iamRole = mapping.flatMap(S3SecurityMappingResult::iamRole).or(() -> staticIamRole);
            String roleSessionName = mapping.flatMap(S3SecurityMappingResult::roleSessionName).orElse(staticRoleSessionName);

            S3ClientBuilder s3 = S3Client.builder();
            s3.overrideConfiguration(overrideConfiguration);
            s3.crossRegionAccessEnabled(config.isCrossRegionAccessEnabled());
            s3.httpClient(httpClient);
            s3.responseChecksumValidation(WHEN_REQUIRED);
            s3.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
            s3.addPlugin(LegacyMd5Plugin.create());

            region.map(Region::of).ifPresent(s3::region);
            endpoint.map(URI::create).ifPresent(s3::endpointOverride);
            s3.forcePathStyle(pathStyleAccess);

            s3.useArnRegion(multiRegionAccessPointsEnabled);
            if (!multiRegionAccessPointsEnabled) {
                s3.disableMultiRegionAccessPoints(true);
            }

            if (useWebIdentityTokenCredentialsProvider) {
                s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else if (iamRole.isPresent()) {
                s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(request -> request
                                .roleArn(iamRole.get())
                                .roleSessionName(roleSessionName)
                                .externalId(externalId))
                        .stsClient(createStsClient(config, credentialsProvider))
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else {
                credentialsProvider.ifPresent(s3::credentialsProvider);
            }

            return s3.build();
        };
    }

    @SuppressWarnings("deprecation")
    private static ClientOverrideConfiguration createOverrideConfiguration(OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(AwsSdkTelemetry.builder(openTelemetry)
                        .setCaptureExperimentalSpanAttributes(true)
                        .setRecordIndividualHttpError(true)
                        .build().newExecutionInterceptor())
                .retryStrategy(getRetryStrategy(config.getRetryMode()).toBuilder()
                        .maxAttempts(config.getMaxErrorRetries())
                        .build())
                .appId(config.getApplicationId())
                .addMetricPublisher(metricPublisher);
        config.getSignerType().ifPresent(signer -> builder.putAdvancedOption(SIGNER, signer.create()));
        config.getApiCallTimeout().map(io.airlift.units.Duration::toMillis).map(Duration::ofMillis).ifPresent(builder::apiCallTimeout);
        config.getApiCallAttemptTimeout().map(io.airlift.units.Duration::toMillis).map(Duration::ofMillis).ifPresent(builder::apiCallAttemptTimeout);
        return builder.build();
    }

    private static SdkHttpClient createHttpClient(S3FileSystemConfig config)
    {
        // Using Multi-Region Access Points requires an HTTP client that supports the SigV4a signing protocol, which is not supported by the Apache HTTP Client.
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html#how-sigv4a-works
        return config.isMultiRegionAccessPointsEnabled()
                ? createAwsCrtHttpClient(config)
                : createApacheHttpClient(config);
    }

    private static SdkHttpClient createApacheHttpClient(S3FileSystemConfig config)
    {
        ApacheHttpClient.Builder client = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections())
                .tcpKeepAlive(config.getTcpKeepAlive());

        config.getConnectionTtl().ifPresent(ttl -> client.connectionTimeToLive(ttl.toJavaTime()));
        config.getConnectionMaxIdleTime().ifPresent(time -> client.connectionMaxIdleTime(time.toJavaTime()));
        config.getSocketConnectTimeout().ifPresent(timeout -> client.connectionTimeout(timeout.toJavaTime()));
        config.getSocketTimeout().ifPresent(timeout -> client.socketTimeout(timeout.toJavaTime()));

        if (config.getHttpProxy() != null) {
            client.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(URI.create("%s://%s".formatted(
                            config.isHttpProxySecure() ? "https" : "http",
                            config.getHttpProxy())))
                    .username(config.getHttpProxyUsername())
                    .password(config.getHttpProxyPassword())
                    .nonProxyHosts(config.getNonProxyHosts())
                    .preemptiveBasicAuthenticationEnabled(config.getHttpProxyPreemptiveBasicProxyAuth())
                    .build());
        }

        return client.build();
    }

    private static SdkHttpClient createAwsCrtHttpClient(S3FileSystemConfig config)
    {
        AwsCrtHttpClient.Builder client = AwsCrtHttpClient.builder()
                .maxConcurrency(config.getMaxConnections());

        config.getConnectionMaxIdleTime().map(io.airlift.units.Duration::toJavaTime).ifPresent(client::connectionMaxIdleTime);
        config.getSocketConnectTimeout().map(io.airlift.units.Duration::toJavaTime).ifPresent(client::connectionTimeout);

        if (config.getHttpProxy() != null) {
            client.proxyConfiguration(software.amazon.awssdk.http.crt.ProxyConfiguration.builder()
                    .scheme(config.isHttpProxySecure() ? "https" : "http")
                    .host(config.getHttpProxy().getHost())
                    .port(config.getHttpProxy().getPort())
                    .username(config.getHttpProxyUsername())
                    .password(config.getHttpProxyPassword())
                    .nonProxyHosts(config.getNonProxyHosts())
                    .build());
        }

        return client.build();
    }

    public static AwsCredentialsProvider getCustomAwsCredentialsProvider(String providerClass, Map<String, String> customCredentialProviderArguments)
    {
        try {
            Class<?> awsCredentialProvider = Class.forName(providerClass);
            checkArgument(AwsCredentialsProvider.class.isAssignableFrom(awsCredentialProvider), "%s is not a subclass of %s", providerClass, AwsCredentialsProvider.class);
            return awsCredentialProvider
                    .asSubclass(AwsCredentialsProvider.class)
                    .getConstructor(new TypeToken<Map<String, String>>(){}.getRawType())
                    .newInstance(customCredentialProviderArguments);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("AwsCredentialsProvider " + providerClass + " not found", e);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Unable to initialize AwsCredentialsProvider " + providerClass, e);
        }
    }

    interface S3ClientFactory
    {
        S3Client create(Optional<S3SecurityMappingResult> mapping);
    }
}
