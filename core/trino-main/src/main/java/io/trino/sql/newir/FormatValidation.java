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
package io.trino.sql.newir;

import java.util.regex.Pattern;

public class FormatValidation
{
    private static final Pattern IDENTIFIER = Pattern.compile("([a-z]|[A-Z]|_)([a-z]|[A-Z]|[0-9]|_)*");
    private static final Pattern PREFIXED_IDENTIFIER = Pattern.compile("([a-z]|[A-Z]|[0-9]|_)+");
    private static final Pattern NAMESPACED_IDENTIFIER = Pattern.compile("([a-z]|[A-Z]|_)([a-z]|[A-Z]|[0-9]|_)*:([a-z]|[A-Z]|_)([a-z]|[A-Z]|[0-9]|_)*");

    private FormatValidation()
    {}

    public static boolean isValidIdentifier(String identifier)
    {
        return IDENTIFIER.matcher(identifier).matches();
    }

    public static boolean isValidPrefixedIdentifier(String identifier)
    {
        return PREFIXED_IDENTIFIER.matcher(identifier).matches();
    }

    public static boolean isValidAttributeName(String name)
    {
        return IDENTIFIER.matcher(name).matches() || NAMESPACED_IDENTIFIER.matcher(name).matches();
    }
}
