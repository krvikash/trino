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
package com.starburstdata.plugin.kdb;

import com.google.inject.Inject;
import com.kx.c;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.plugin.kdb.KdbErrorCode.KDB_CONNECTION_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

public class KdbConnectionFactory
{
    private final String host;
    private final int port;
    private final CredentialProvider credentialProvider;

    @Inject
    public KdbConnectionFactory(CredentialProvider credentialProvider, KdbConfig config)
    {
        host = config.getHost();
        port = config.getPort();
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
    }

    public c openConnection(ConnectorSession session)
    {
        Optional<String> user = credentialProvider.getConnectionUser(Optional.of(session.getIdentity()));
        Optional<String> password = credentialProvider.getConnectionPassword(Optional.of(session.getIdentity()));
        try {
            if (user.isPresent()) {
                return new c(host, port, "%s:%s".formatted(user.orElse(""), password.orElse("")));
            }
            return new c(host, port);
        }
        catch (c.KException e) {
            throw new TrinoException(PERMISSION_DENIED, "KDB+ refused connection at %s:%d (KDB+ error: %s)".formatted(host, port, e.getMessage()), e);
        }
        catch (IOException e) {
            throw new TrinoException(KDB_CONNECTION_ERROR, "Cannot connect to KDB+ at %s:%d: %s".formatted(host, port, e.getMessage()), e);
        }
    }
}
