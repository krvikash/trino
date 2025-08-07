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

package com.starburstdata.trino.plugin.base.cache.identity;

import io.trino.plugin.base.cache.identity.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AuthenticationBasedIdentityCacheMapping
        implements IdentityCacheMapping
{
    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        return new Key(session.getIdentity().getUser(), session.getIdentity().getPrincipal().map(Principal::getName));
    }

    private static class Key
            extends IdentityCacheKey
    {
        private final String user;
        private final Optional<String> principalName;

        public Key(String user, Optional<String> principalName)
        {
            this.user = requireNonNull(user, "user is null");
            this.principalName = requireNonNull(principalName, "principalName is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(user, key.user) && Objects.equals(principalName, key.principalName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, principalName);
        }
    }
}
