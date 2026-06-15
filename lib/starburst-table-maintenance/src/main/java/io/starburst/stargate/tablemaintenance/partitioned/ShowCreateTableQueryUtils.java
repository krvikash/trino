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
package io.starburst.stargate.tablemaintenance.partitioned;

import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ShowCreateTableQueryUtils
{
    private static final Pattern FORMAT_VERSION_PATTERN = Pattern.compile("format_version\\s*=\\s*(\\d+)");

    private ShowCreateTableQueryUtils() {}

    public static OptionalInt parseFormatVersion(String showCreateTableOutput)
    {
        Matcher matcher = FORMAT_VERSION_PATTERN.matcher(showCreateTableOutput);
        if (matcher.find()) {
            return OptionalInt.of(Integer.parseInt(matcher.group(1)));
        }
        return OptionalInt.empty();
    }
}
