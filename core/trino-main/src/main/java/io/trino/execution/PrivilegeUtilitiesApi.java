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
package io.trino.execution;

import io.starburst.stargate.id.EntityKind;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;

public interface PrivilegeUtilitiesApi
{
    Set<Privilege> parseStatementPrivileges(Node statement, Optional<List<String>> optionalPrivileges, EntityKind entityKind);

    /**
     * Validates {@code schemaName} when it denotes a wildcard target.
     *
     * @return {@code true} if the name is a wildcard and has been validated, in which case the caller
     *         should skip normal object-existence checks; {@code false} otherwise. Wildcards are a Galaxy
     *         concept, so the default implementation always returns {@code false}.
     */
    default boolean validatedAsWildcard(Session session, Metadata metadata, Statement statement, CatalogSchemaName schemaName)
    {
        return false;
    }

    /**
     * Validates {@code tableName} when it denotes a wildcard target.
     *
     * @return {@code true} if the name is a wildcard and has been validated, in which case the caller
     *         should skip normal object-existence checks; {@code false} otherwise. Wildcards are a Galaxy
     *         concept, so the default implementation always returns {@code false}.
     */
    default boolean validatedAsWildcard(Session session, Metadata metadata, Statement statement, QualifiedObjectName tableName)
    {
        return false;
    }

    static Set<EntityPrivilege> fetchEntityKindPrivileges(String entityKind, Metadata metadata, Optional<List<String>> privileges)
    {
        Set<EntityPrivilege> allPrivileges = metadata.getAllEntityKindPrivileges(entityKind);
        if (privileges.isPresent()) {
            return privileges.get().stream()
                    .map(privilege -> {
                        EntityPrivilege entityPrivilege = new EntityPrivilege(privilege.toUpperCase(Locale.ENGLISH));
                        if (!allPrivileges.contains(entityPrivilege)) {
                            throw new TrinoException(INVALID_PRIVILEGE, "Privilege %s is not supported for entity kind %s".formatted(privilege, entityKind));
                        }
                        return entityPrivilege;
                    }).collect(toImmutableSet());
        }
        else {
            return allPrivileges;
        }
    }
}
