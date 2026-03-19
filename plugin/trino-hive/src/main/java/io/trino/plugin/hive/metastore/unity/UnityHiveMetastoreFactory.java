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
package io.trino.plugin.hive.metastore.unity;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.OptionalInt;

public class UnityHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final HiveMetastore metastore;

    // Unity metastore does not support impersonation, so just use single shared instance
    @Inject
    public UnityHiveMetastoreFactory(
            UnityMetastoreConfig config,
            Optional<UnityMetastoreProxyConfig> proxyConfig,
            Tracer tracer,
            SupportedUnityTableFormatsProvider supportedUnityTableFormatsProvider)
    {
        this.metastore = new TracingUnityHiveMetastore(
                tracer,
                new UnityHiveMetastore(
                        config.getHost(),
                        config.getCatalogName(),
                        config.getToken(),
                        config.isVendedCredentialsEnabled(),
                        config.isProxyEnabled(),
                        proxyConfig.map(UnityMetastoreProxyConfig::getProxyHost),
                        proxyConfig.map(UnityMetastoreProxyConfig::getProxyPort).map(OptionalInt::of).orElseGet(OptionalInt::empty),
                        proxyConfig.flatMap(UnityMetastoreProxyConfig::getUsername),
                        proxyConfig.flatMap(UnityMetastoreProxyConfig::getPassword),
                        proxyConfig.map(UnityMetastoreProxyConfig::getNonProxyHosts),
                        supportedUnityTableFormatsProvider.supportedUnityTableFormats()));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return metastore;
    }
}
