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

import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.deltalake.metastore.unity.dynamic.DynamicUnityMetastoreConfig;
import io.trino.plugin.hive.metastore.unity.SupportedUnityTableFormatsProvider;
import io.trino.plugin.hive.metastore.unity.TracingUnityHiveMetastore;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.databricks.sdk.core.PatCredentialsProvider.PAT;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeDynamicUnityMetastoreFactory
        implements HiveMetastoreFactory
{
    private final String unityHostCredentialName;
    private final String unityTokenCredentialName;
    private final String unityCatalogNameCredentialName;
    private final Optional<String> vendedCredentialsCredentialName;
    private final boolean vendedCredentialsEnabledDefault;
    private final Tracer tracer;
    private final Set<DataSourceFormat> supportedUnityTableFormats;

    @Inject
    public DeltaLakeDynamicUnityMetastoreFactory(
            DynamicUnityMetastoreConfig unityCredentialConfig,
            Tracer tracer,
            SupportedUnityTableFormatsProvider supportedUnityTableFormatsProvider)
    {
        this.unityHostCredentialName = unityCredentialConfig.getUnityHostCredentialName();
        this.unityTokenCredentialName = unityCredentialConfig.getUnityTokenCredentialName();
        this.unityCatalogNameCredentialName = unityCredentialConfig.getUnityCatalogNameCredentialName();
        this.vendedCredentialsCredentialName = unityCredentialConfig.getVendedCredentialsCredentialName();
        this.vendedCredentialsEnabledDefault = unityCredentialConfig.isVendedCredentialsEnabled();
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.supportedUnityTableFormats = requireNonNull(supportedUnityTableFormatsProvider, "supportedUnityTableFormatsProvider is null")
                .supportedUnityTableFormats();
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return true;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        ConnectorIdentity connectorIdentity = identity.orElseThrow(() ->
                new TrinoException(GENERIC_USER_ERROR, "Identity must be provided for dynamic Unity Catalog connection"));
        Map<String, String> extraCredentials = connectorIdentity.getExtraCredentials();

        String host = getRequiredCredential(extraCredentials, unityHostCredentialName);
        String token = getRequiredCredential(extraCredentials, unityTokenCredentialName);
        String catalogName = getRequiredCredential(extraCredentials, unityCatalogNameCredentialName);
        boolean vendedCredentialsEnabled = resolveVendedCredentialsEnabled(extraCredentials);

        return new TracingUnityHiveMetastore(
                tracer,
                new UnityHiveMetastore(
                        host,
                        catalogName,
                        Optional.of(token),
                        PAT,
                        Optional.empty(),
                        vendedCredentialsEnabled,
                        false,
                        Optional.empty(),
                        OptionalInt.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        supportedUnityTableFormats));
    }

    boolean resolveVendedCredentialsEnabled(Map<String, String> extraCredentials)
    {
        return vendedCredentialsCredentialName
                .map(extraCredentials::get)
                .map(value -> "true".equalsIgnoreCase(value))
                .orElse(vendedCredentialsEnabledDefault);
    }

    private static String getRequiredCredential(Map<String, String> extraCredentials, String credentialName)
    {
        String value = extraCredentials.get(credentialName);
        if (value == null) {
            throw new TrinoException(GENERIC_USER_ERROR, "Extra credential '%s' must be provided".formatted(credentialName));
        }
        return value;
    }
}
