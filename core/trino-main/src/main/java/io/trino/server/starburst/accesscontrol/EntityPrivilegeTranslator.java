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
package io.trino.server.starburst.accesscontrol;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.EntityKind;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.EntityPrivilege;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.getEntityKindPrivileges;
import static io.trino.spi.StandardErrorCode.INVALID_ENTITY_KIND;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;

public class EntityPrivilegeTranslator
{
    private EntityPrivilegeTranslator() {}

    public static Set<EntityPrivilege> getPrivilegesForEntityKind(String entityKindName)
    {
        return getEntityKindPrivileges(translateEntityKind(entityKindName)).stream()
                .map(privilege -> new EntityPrivilege(privilege.name()))
                .collect(toImmutableSet());
    }

    public static Set<Privilege> translateEntityPrivileges(EntityKind entityKind, Set<EntityPrivilege> privileges)
    {
        ImmutableSet.Builder<Privilege> builder = ImmutableSet.builder();
        for (EntityPrivilege entityPrivilege : privileges) {
            Optional<Privilege> optionalPrivilege = getPrivilegeOrNull(entityPrivilege.name());
            if (optionalPrivilege.isEmpty()) {
                throw new TrinoException(INVALID_PRIVILEGE, "Unrecognized privilege " + entityPrivilege.name());
            }
            Privilege privilege = optionalPrivilege.get();
            if (!privilege.isValidEntityKind(entityKind)) {
                throw new TrinoException(INVALID_PRIVILEGE, "Privilege %s may not be used with entity kind %s".formatted(entityPrivilege.name(), entityKind.name()));
            }
            builder.add(privilege);
        }
        return builder.build();
    }

    public static EntityKindAndPrivileges translateEntityKindAndPrivileges(String entityKindName, Set<EntityPrivilege> privileges)
    {
        Optional<EntityKind> optionalEntityKind = getEntityKind(entityKindName);
        if (optionalEntityKind.isEmpty()) {
            throw new TrinoException(INVALID_ENTITY_KIND, "Unrecognized entity kind " + entityKindName);
        }
        EntityKind entityKind = optionalEntityKind.get();
        Set<Privilege> translatedPrivileges = translateEntityPrivileges(entityKind, privileges);
        return new EntityKindAndPrivileges(entityKind, translatedPrivileges);
    }

    public static EntityKind translateEntityKind(String entityKindName)
    {
        Optional<EntityKind> entityKind = getEntityKind(entityKindName);
        if (entityKind.isEmpty()) {
            throw new TrinoException(INVALID_ENTITY_KIND, "Unrecognized entity kind " + entityKindName);
        }
        return entityKind.get();
    }

    public static Optional<Privilege> getPrivilegeOrNull(String privilegeName)
    {
        for (Privilege privilege : Privilege.values()) {
            if (privilege.name().equalsIgnoreCase(privilegeName)) {
                return Optional.of(privilege);
            }
        }
        return Optional.empty();
    }

    public static Optional<EntityKind> getEntityKind(String entityKindName)
    {
        // Special case - - recognize "my" as entity kind ACCOUNT
        if ("my".equalsIgnoreCase(entityKindName)) {
            return Optional.of(EntityKind.ACCOUNT);
        }
        for (EntityKind entityKind : EntityKind.values()) {
            if (entityKind.name().equalsIgnoreCase(entityKindName)) {
                return Optional.of(entityKind);
            }
        }
        return Optional.empty();
    }
}
