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
package io.trino.plugin.elasticsearch.client;

import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.AwsSecurityConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Optional;

public class AwsSecurityRestClientConfigurator
        implements ElasticRestClientConfigurator
{
    private final String region;
    private final Optional<String> accessKey;
    private final Optional<String> secretKey;
    private final Optional<String> iamRole;
    private final Optional<String> externalId;

    @Inject
    AwsSecurityRestClientConfigurator(AwsSecurityConfig awsSecurityConfig)
    {
        this.region = awsSecurityConfig.getRegion();
        this.accessKey = awsSecurityConfig.getAccessKey();
        this.secretKey = awsSecurityConfig.getSecretKey();
        this.iamRole = awsSecurityConfig.getIamRole();
        this.externalId = awsSecurityConfig.getExternalId();
    }

    @Override
    public void configure(HttpAsyncClientBuilder clientBuilder)
    {
        clientBuilder.addInterceptorLast(new AwsRequestSigner(region, getAwsCredentialsProvider()));
    }

    private AwsCredentialsProvider getAwsCredentialsProvider()
    {
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        if (accessKey.isPresent() && secretKey.isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    accessKey.get(),
                    secretKey.get()));
        }

        if (iamRole.isPresent()) {
            StsAssumeRoleCredentialsProvider.Builder credentialsProviderBuilder = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder()
                            .region(Region.of(region))
                            .credentialsProvider(credentialsProvider)
                            .build())
                    .refreshRequest(request -> {
                        request
                                .roleArn(iamRole.get())
                                .roleSessionName("trino-session");
                        externalId.ifPresent(request::externalId);
                    });
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }
}
