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
package io.trino.hive.formats.line.csv;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static io.trino.hive.formats.HiveClassNames.OPENCSV_SERDE_CLASS;

public final class CsvConstants
{
    static final Set<String> HIVE_SERDE_CLASS_NAMES = ImmutableSet.of(OPENCSV_SERDE_CLASS);
    static final String SEPARATOR_KEY = "separatorChar";
    public static final String QUOTE_KEY = "quoteChar";
    public static final String ESCAPE_KEY = "escapeChar";
    public static final String MULTILINE_KEY = "multiline";
    public static final String LINE_SEPARATOR_KEY = "lineSeparator";
    static final byte DEFAULT_SEPARATOR = ',';
    public static final byte DEFAULT_QUOTE = '\"';
    public static final byte DESERIALIZER_DEFAULT_ESCAPE = '\\';
    // NOTE: serializer and deserializer use different escape characters which can result in data
    // that does not roundtrip when quote character is set but escape it not set.
    static final byte SERIALIZER_DEFAULT_ESCAPE = '\"';

    private CsvConstants() {}

    static char getCharProperty(Map<String, String> schema, String key, byte defaultValue)
    {
        String value = schema.get(key);
        if (value == null) {
            return (char) defaultValue;
        }
        // Hive allows the property to be longer than a single character, so we preserve that behavior
        return value.charAt(0);
    }

    public static byte getByteProperty(Map<String, String> schema, String key, byte defaultValue)
    {
        char c = getCharProperty(schema, key, defaultValue);
        // Trino restrict special characters to ASCII
        if (c > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("%s must be 7-bit ASCII: %02x".formatted(key, (short) c));
        }
        return (byte) c;
    }
}
