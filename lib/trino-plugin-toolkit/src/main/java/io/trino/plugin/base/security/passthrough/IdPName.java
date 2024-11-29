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
package io.trino.plugin.base.security.passthrough;

import java.util.Locale;
import java.util.Objects;

import static com.google.common.base.CharMatcher.anyOf;
import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IdPName
{
    private final String name;

    private IdPName(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public static IdPName of(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(inRange('a', 'z').or(inRange('0', '9'))
                .or(anyOf("-_"))
                .matchesAllOf(name), "not valid name %s given. It can only consist of a-z, 0-9 and \"-\" or \"_\" characters", name);

        return new IdPName(name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IdPName idPName)) {
            return false;
        }
        return name.equals(idPName.name);
    }

    public boolean hasName(String name)
    {
        requireNonNull(name, "name is null");

        return this.name.equals(name.toUpperCase(Locale.US));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    public String asString()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return asString();
    }
}
