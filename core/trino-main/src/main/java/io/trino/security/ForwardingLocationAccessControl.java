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
package io.trino.security;

import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.LocationAccessControl;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingLocationAccessControl
        implements LocationAccessControl
{
    public static ForwardingLocationAccessControl of(Supplier<LocationAccessControl> locationAccessControlSupplier)
    {
        requireNonNull(locationAccessControlSupplier, "locationAccessControlSupplier is null");
        return new ForwardingLocationAccessControl()
        {
            @Override
            protected LocationAccessControl delegate()
            {
                return requireNonNull(locationAccessControlSupplier.get(), "locationAccessControlSupplier.get() is null");
            }
        };
    }

    protected abstract LocationAccessControl delegate();

    @Override
    public void checkCanUseLocation(ConnectorIdentity identity, String location, String queryId)
    {
        delegate().checkCanUseLocation(identity, location, queryId);
    }
}
