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

import static java.util.Objects.requireNonNull;

public enum SystemAccessControlOperation
{
    CATALOG_MANAGEMENT("catalog management"),
    QUERY_MANAGEMENT("query management"),
    SYSTEM_INFORMATION("system information"),
    USER_IMPERSONATION("user impersonation"),
    SYSTEM_ROLES("system roles"),
    ENTITY_PRIVILEGES("entity privileges"),
    SYSTEM_SESSION_PROPERTIES("system session properties"),
    ENTITY_AUTHORIZATION("entity authorization");

    private final String description;

    SystemAccessControlOperation(String description)
    {
        this.description = requireNonNull(description, "description is null");
    }

    /**
     * Human-readable name of this family, for error messages.
     */
    public String description()
    {
        return description;
    }
}
