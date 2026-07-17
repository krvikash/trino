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
package io.trino.server.starburst.security;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.RoleName;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record DisplayedGrant(EntityKind entityKind, List<String> qualifiedEntityNameParts, GrantKind grantKind, Optional<RoleName> owner, boolean isExplicitOwner, RoleName grantee, Privilege privilege, boolean isGrantable)
{
    public DisplayedGrant
    {
        requireNonNull(entityKind, "entityKind is null");
        qualifiedEntityNameParts = ImmutableList.copyOf(requireNonNull(qualifiedEntityNameParts, "qualifiedEntityNameParts is null"));
        requireNonNull(grantKind, "grantKind is null");
        requireNonNull(grantee, "grantee is null");
        checkArgument(!grantee.getName().isBlank(), "grantee is blank");
        requireNonNull(privilege, "privilege is null");
    }
}
