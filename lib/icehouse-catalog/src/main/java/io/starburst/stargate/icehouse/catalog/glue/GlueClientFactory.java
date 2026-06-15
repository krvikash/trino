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
package io.starburst.stargate.icehouse.catalog.glue;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.glue.GlueCatalogIdInterceptor;
import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class GlueClientFactory
{
    private final Set<ExecutionInterceptor> executionInterceptors;

    @Inject
    public GlueClientFactory(@ForGlueClient Set<ExecutionInterceptor> executionInterceptors)
    {
        this.executionInterceptors = requireNonNull(executionInterceptors, "executionInterceptors is null");
    }

    public GlueClient createFromProperties(GlueClientConfig config)
    {
        return createGlueClient(config);
    }

    private GlueClient createGlueClient(GlueClientConfig config)
    {
        GlueClientBuilder glue = GlueClient.builder();
        ImmutableList.Builder<ExecutionInterceptor> allInterceptors = ImmutableList.<ExecutionInterceptor>builder()
                .addAll(executionInterceptors);

        config.getCatalogId().ifPresent(catalogId -> allInterceptors.add(new GlueCatalogIdInterceptor(catalogId)));

        glue.overrideConfiguration(builder -> builder
                .executionInterceptors(allInterceptors.build())
                .retryStrategy(retryBuilder -> retryBuilder
                        .backoffStrategy(BackoffStrategy.exponentialDelay(
                                Duration.ofMillis(20),
                                Duration.ofMillis(1500)))
                        .maxAttempts(config.getMaxGlueErrorRetries())));

        Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);

        if (config.isUseWebIdentityTokenCredentialsProvider()) {
            glue.credentialsProvider(StsWebIdentityTokenFileCredentialsProvider.builder()
                    .stsClient(getStsClient(config, staticCredentialsProvider))
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }
        else if (config.getIamRole().isPresent()) {
            glue.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(config.getIamRole().get())
                            .roleSessionName("trino-session")
                            .externalId(config.getExternalId().orElse(null)))
                    .stsClient(getStsClient(config, staticCredentialsProvider))
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }
        else {
            staticCredentialsProvider.ifPresent(glue::credentialsProvider);
        }

        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(config.getMaxGlueConnections());

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glue.region(Region.of(config.getGlueRegion().get()));
            glue.endpointOverride(config.getGlueEndpointUrl().get());
        }
        else if (config.getGlueRegion().isPresent()) {
            glue.region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            glue.region(DefaultAwsRegionProviderChain.builder().build().getRegion());
        }

        glue.httpClientBuilder(httpClient);

        return glue.build();
    }

    private static Optional<StaticCredentialsProvider> getStaticCredentialsProvider(GlueClientConfig config)
    {
        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get())));
        }
        return Optional.empty();
    }

    private static StsClient getStsClient(GlueClientConfig config, Optional<StaticCredentialsProvider> staticCredentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        staticCredentialsProvider.ifPresent(sts::credentialsProvider);

        if (config.getGlueStsEndpointUrl().isPresent() && config.getGlueStsRegion().isPresent()) {
            sts.endpointOverride(config.getGlueStsEndpointUrl().get())
                    .region(Region.of(config.getGlueStsRegion().get()));
        }
        else if (config.getGlueStsRegion().isPresent()) {
            sts.region(Region.of(config.getGlueStsRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            sts.region(DefaultAwsRegionProviderChain.builder().build().getRegion());
        }

        return sts.build();
    }
}
