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

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record EntityKindAndPrivileges(EntityKind entityKind, Set<Privilege> privileges)
{
    public EntityKindAndPrivileges
    {
        requireNonNull(entityKind, "entityKind is null");
        privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
    }
}
