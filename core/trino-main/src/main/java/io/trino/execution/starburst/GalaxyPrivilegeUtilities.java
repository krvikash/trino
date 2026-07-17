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
package io.trino.execution.starburst;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.id.EntityKind;
import io.trino.Session;
import io.trino.execution.PrivilegeUtilitiesApi;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public final class GalaxyPrivilegeUtilities
        implements PrivilegeUtilitiesApi
{
    private static final Map<EntityKind, Set<Privilege>> ENTITY_KIND_PRIVILEGES = ImmutableMap.of(
            EntityKind.SCHEMA, ImmutableSet.of(Privilege.CREATE),
            EntityKind.TABLE, ImmutableSet.of(Privilege.SELECT, Privilege.UPDATE, Privilege.INSERT, Privilege.DELETE, Privilege.MANAGE_DATA_OBSERVABILITY, Privilege.CREATE_BRANCH));

    public static Set<Privilege> getPrivilegesForEntityKind(EntityKind entityKind)
    {
        Set<Privilege> privileges = ENTITY_KIND_PRIVILEGES.get(entityKind);
        if (privileges != null) {
            return privileges;
        }
        throw new IllegalArgumentException("Could not find privileges for EntityKind." + entityKind);
    }

    @Override
    public Set<Privilege> parseStatementPrivileges(Node statement, Optional<List<String>> optionalPrivileges, EntityKind entityKind)
    {
        Set<Privilege> privileges;
        if (optionalPrivileges.isPresent()) {
            privileges = optionalPrivileges.get().stream()
                    .map(privilege -> parsePrivilege(statement, privilege, entityKind))
                    .collect(toImmutableSet());
        }
        else {
            // All privileges
            privileges = getPrivilegesForEntityKind(entityKind);
        }
        return privileges;
    }

    private static Privilege parsePrivilege(Node statement, String privilegeString, EntityKind entityKind)
    {
        for (Privilege privilege : getPrivilegesForEntityKind(entityKind)) {
            if (privilege.toString().equalsIgnoreCase(privilegeString)) {
                return privilege;
            }
        }

        throw semanticException(INVALID_PRIVILEGE, statement, "Unknown privilege: '%s'", privilegeString);
    }

    @Override
    public boolean validatedAsWildcard(Session session, Metadata metadata, Statement statement, CatalogSchemaName schemaName)
    {
        if (!isWildcard(schemaName)) {
            return false;
        }
        if (isWildcard(schemaName.getCatalogName())) {
            throw semanticException(SYNTAX_ERROR, statement, "Catalog wildcard is not allowed: %s", schemaName);
        }
        if (!metadata.catalogExists(session, schemaName.getCatalogName())) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", schemaName.getCatalogName());
        }
        return true;
    }

    @Override
    public boolean validatedAsWildcard(Session session, Metadata metadata, Statement statement, QualifiedObjectName tableName)
    {
        if (!isWildcard(tableName)) {
            return false;
        }
        if (isWildcard(tableName.catalogName())) {
            throw semanticException(SYNTAX_ERROR, statement, "Catalog wildcard is not allowed: %s", tableName);
        }

        if (isWildcard(tableName.schemaName())) {
            if (!isWildcard(tableName.asSchemaTableName().getTableName())) {
                throw semanticException(SYNTAX_ERROR, statement, "Schema wildcard requires a table wildcard: %s", tableName);
            }
            if (!metadata.catalogExists(session, tableName.catalogName())) {
                throw semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", tableName.catalogName());
            }
        }
        else if (isWildcard(tableName.objectName())) {
            CatalogSchemaName schemaName = new CatalogSchemaName(tableName.catalogName(), tableName.schemaName());
            if (!metadata.schemaExists(session, schemaName)) {
                throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", schemaName);
            }
        }
        return true;
    }

    private static boolean isWildcard(CatalogSchemaName schemaName)
    {
        return isWildcard(schemaName.getCatalogName()) || isWildcard(schemaName.getSchemaName());
    }

    private static boolean isWildcard(QualifiedObjectName tableName)
    {
        return isWildcard(tableName.catalogName()) || isWildcard(tableName.schemaName()) || isWildcard(tableName.objectName());
    }

    private static boolean isWildcard(String name)
    {
        return name.equals("*");
    }
}
