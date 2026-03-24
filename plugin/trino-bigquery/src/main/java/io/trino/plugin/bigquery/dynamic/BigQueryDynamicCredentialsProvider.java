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
package io.trino.plugin.bigquery.dynamic;

import com.google.auth.Credentials;
import com.google.inject.Inject;
import io.trino.plugin.bigquery.BigQueryCredentialsSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.bigquery.StaticBigQueryCredentialsSupplier.createCredentialsFromKey;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class BigQueryDynamicCredentialsProvider
        implements BigQueryCredentialsSupplier
{
    private final String credentialsKeyCredentialName;

    @Inject
    public BigQueryDynamicCredentialsProvider(BigQueryDynamicConnectionPassthroughConfig config)
    {
        this.credentialsKeyCredentialName = config.getCredentialsKeyCredentialName();
    }

    @Override
    public Optional<Credentials> getCredentials(ConnectorSession session)
    {
        Map<String, String> extraCredentials = session.getIdentity().getExtraCredentials();
        if (!extraCredentials.containsKey(credentialsKeyCredentialName)) {
            throw new TrinoException(
                    GENERIC_USER_ERROR,
                    "Extra credential '" + credentialsKeyCredentialName + "' must be provided");
        }
        Credentials credentialsKey = createCredentialsFromKey(Optional.empty(), extraCredentials.get(credentialsKeyCredentialName));
        return Optional.of(credentialsKey);
    }
}
