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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.deltalake.metastore.unity.dynamic.DynamicUnityMetastoreConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDeltaLakeDynamicUnityMetastoreFactory
{
    private static final Tracer TRACER = TestingTelemetry.create("test-dynamic-unity-metastore-factory").getTracer();

    private static final String HOST_CREDENTIAL_NAME = "dynamic_host";
    private static final String TOKEN_CREDENTIAL_NAME = "dynamic_token";
    private static final String CATALOG_CREDENTIAL_NAME = "dynamic_catalog";

    private static final Map<String, String> ALL_CREDENTIALS = ImmutableMap.of(
            HOST_CREDENTIAL_NAME, "example.databricks.com",
            TOKEN_CREDENTIAL_NAME, "token",
            CATALOG_CREDENTIAL_NAME, "my_catalog");

    @Test
    void testIsImpersonationEnabled()
    {
        assertThat(createFactory().isImpersonationEnabled()).isTrue();
    }

    @Test
    void testCreateMetastoreSucceedsWithAllCredentials()
    {
        ConnectorIdentity identity = ConnectorIdentity.forUser("user")
                .withExtraCredentials(ALL_CREDENTIALS)
                .build();

        HiveMetastore metastore = createFactory().createMetastore(Optional.of(identity));

        assertThat(metastore).isNotNull();
    }

    @Test
    void testCreateMetastoreFailsWithoutIdentity()
    {
        assertThatThrownBy(() -> createFactory().createMetastore(Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Identity must be provided for dynamic Unity Catalog connection")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
    }

    @Test
    void testCreateMetastoreFailsWithEmptyExtraCredentials()
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("user");

        assertThatThrownBy(() -> createFactory().createMetastore(Optional.of(identity)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Extra credential '%s' must be provided".formatted(HOST_CREDENTIAL_NAME))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
    }

    @Test
    void testCreateMetastoreFailsWithMissingHostCredential()
    {
        ConnectorIdentity identity = ConnectorIdentity.forUser("user")
                .withExtraCredentials(ImmutableMap.of(
                        TOKEN_CREDENTIAL_NAME, "token",
                        CATALOG_CREDENTIAL_NAME, "my_catalog"))
                .build();

        assertThatThrownBy(() -> createFactory().createMetastore(Optional.of(identity)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Extra credential '%s' must be provided".formatted(HOST_CREDENTIAL_NAME))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
    }

    @Test
    void testCreateMetastoreFailsWithMissingTokenCredential()
    {
        ConnectorIdentity identity = ConnectorIdentity.forUser("user")
                .withExtraCredentials(ImmutableMap.of(
                        HOST_CREDENTIAL_NAME, "example.databricks.com",
                        CATALOG_CREDENTIAL_NAME, "my_catalog"))
                .build();

        assertThatThrownBy(() -> createFactory().createMetastore(Optional.of(identity)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Extra credential '%s' must be provided".formatted(TOKEN_CREDENTIAL_NAME))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
    }

    @Test
    void testCreateMetastoreFailsWithMissingCatalogNameCredential()
    {
        ConnectorIdentity identity = ConnectorIdentity.forUser("user")
                .withExtraCredentials(ImmutableMap.of(
                        HOST_CREDENTIAL_NAME, "example.databricks.com",
                        TOKEN_CREDENTIAL_NAME, "token"))
                .build();

        assertThatThrownBy(() -> createFactory().createMetastore(Optional.of(identity)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Extra credential '%s' must be provided".formatted(CATALOG_CREDENTIAL_NAME))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
    }

    private static DeltaLakeDynamicUnityMetastoreFactory createFactory()
    {
        return new DeltaLakeDynamicUnityMetastoreFactory(
                new DynamicUnityMetastoreConfig()
                        .setUnityHostCredentialName(HOST_CREDENTIAL_NAME)
                        .setUnityTokenCredentialName(TOKEN_CREDENTIAL_NAME)
                        .setUnityCatalogNameCredentialName(CATALOG_CREDENTIAL_NAME),
                TRACER,
                ImmutableSet::of);
    }
}
