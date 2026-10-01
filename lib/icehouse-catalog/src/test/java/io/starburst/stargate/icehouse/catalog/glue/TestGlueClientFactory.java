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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGlueClientFactory
{
    @Test
    public void testClosesStsClientCreatedForAssumeRole()
            throws IOException
    {
        GlueClientConfig config = new GlueClientConfig()
                .setGlueRegion("us-east-1")
                .setGlueStsRegion("us-east-1")
                // nothing listens here, so an open STS client fails to connect
                .setGlueStsEndpointUrl(URI.create("http://127.0.0.1:1"))
                .setIamRole("arn:aws:iam::123456789012:role/test")
                .setAwsAccessKey("access")
                .setAwsSecretKey("secret");

        Closer closer = Closer.create();
        IdentityProvider<?> credentialsProvider;
        try {
            GlueClient glueClient = new GlueClientFactory(ImmutableSet.of()).createFromProperties(config, closer);
            closer.register(glueClient::close);
            credentialsProvider = glueClient.serviceClientConfiguration().credentialsProvider();

            assertThat(resolveFailure(credentialsProvider)).isInstanceOf(SdkClientException.class);
        }
        finally {
            closer.close();
        }

        assertThat(resolveFailure(credentialsProvider)).isInstanceOf(IllegalStateException.class);
    }

    private static Throwable resolveFailure(IdentityProvider<?> credentialsProvider)
    {
        try {
            credentialsProvider.resolveIdentity().join();
        }
        catch (CompletionException e) {
            return e.getCause();
        }
        catch (RuntimeException e) {
            return e;
        }
        throw new AssertionError("Expected credentials resolution to fail");
    }
}
