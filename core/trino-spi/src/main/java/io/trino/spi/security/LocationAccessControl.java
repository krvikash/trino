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
package io.trino.spi.security;

import java.util.Map;

public interface LocationAccessControl
{
    String DEFAULT_NAME = "default";
    LocationAccessControl ALLOW_ALL = new LocationAccessControl() {};

    default void checkCanUseLocation(ConnectorIdentity identity, String location) {}

    class DefaultFactory
            implements LocationAccessControlFactory
    {
        @Override
        public String getName()
        {
            return DEFAULT_NAME;
        }

        @Override
        public LocationAccessControl create(Map<String, String> config, LocationAccessControlFactoryContext context)
        {
            if (!config.isEmpty()) {
                throw new IllegalArgumentException("This location access controller does not support any configuration properties");
            }
            return ALLOW_ALL;
        }
    }
}
